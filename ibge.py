from ibgeparser.enums import Anos, Estados, Modalidades
from ibgeparser.log import _Log as Log
import os
import tempfile
import numpy as np
import glob
from numba import jit
import pandas as pd
from pandas import read_excel
from collections import namedtuple
from array import array
log = Log()
log.init()
DIRETORIO_SAIDA='saida-microdados'
ARQUIVO_DOCUMENTACAO = 'documentacao/Layout/Layout_microdados_Amostra.xls'
diretorio_textos ='listaestados'
arquivos = glob.glob('listaestados/*.txt')

class Microdados:


    """def __criar_pasta_temporaria(self):
        try:
            return tempfile.mkdtemp()
        except OSError as e :
            print('Falha ao criar pasta temporaria: {}'.format(e))

    def __remover_pasta_temporaria(self, pasta_temporaria:str):
        try:
            shutil.rmtree(pasta_temporaria)
        except OSError as e:
            print('Falha ao remover pasta temporaria: {}'.format(e))"""

    def __obter_diretorio_trabalho(self):
        try:
            pasta_trabalho = os.path.join(os.getcwd(), DIRETORIO_SAIDA)
            if not os.path.exists(pasta_trabalho):
                os.makedirs(pasta_trabalho)

            return pasta_trabalho
        except OSError as e:
            print('Falha ao obter pasta de trabalho: {}'.format(e))
        except Exception as e:
            print('Falha ao obter pasta de trabalho: {}'.format(e))

    def __validar_enums(self, ano:enumerate=Anos.DEZ, estados:list=[], modalidades:list=[]):
        if Estados.TODOS in estados:
            estados = list(Estados)[:-1]

        if Modalidades.TODOS in modalidades:
            modalidades = list(Modalidades)[:-1]

        return ano, estados, modalidades

    #@jit(nopython = True)
    def __obter_dados_documentacao(self, pasta_trabalho:str, modalidades:array, ano:str):
            # extraindo csv de documentação para as modalidades escolhidas

        div_columns, ibge_datasets= {}, {}


        for enum_modalidade in modalidades:
            #tr:
            valor_modalidade, descricao_modalidade = enum_modalidade.value

            print(type(valor_modalidade))
            print(type(descricao_modalidade))


            arquivo_csv = os.path.join(pasta_trabalho, 'Documentacao_.csv'.format(descricao_modalidade))

            print('Arquivo de documentacao convertido para csv gerado em: {}'.format(arquivo_csv))

            ibge_desc=read_excel(open(ARQUIVO_DOCUMENTACAO, 'rb'), sheet_name=valor_modalidade, header=1)

            ibge_desc.dropna(how='all', axis='columns')
            ibge_desc.to_csv(arquivo_csv, encoding="utf-8-sig")

            col_specification = list(zip(ibge_desc['POSIÇÃO INICIAL']-1, ibge_desc['POSIÇÃO FINAL']))
            #Div_Columns = namedtuple('colunas', 'descricao_modalidade')
            #coluna = Div_Columns(descricao_modalidade = col_specification)
            #Ibge_Datasets = namedtuple('datasets', 'descricao_modalidade')
            #datasets = Ibge_Datasets(descricao_modalidade = ibge_desc
            ibge_datasets[descricao_modalidade] = ibge_desc
            div_columns[descricao_modalidade] = col_specification


            #except Exception as e:
                #print('Error at acessing the datas of documentation: {}'.format(e))

        return ibge_datasets, div_columns
    def obter_dados_ibge(self, ano:enumerate, estados:array, modalidades:array, header:bool=True):

        cabecalho = ['UF', 'Municipios', 'Área de Ponderação', 'Controle', 'Peso Amostral', 'Região Geográfica', 'Mesorregião', 'Microrregião',
                            'Código da Região Metropolitana', 'Situação do Domicilio', 'Espécie de Unidade Visitada','Tipo de Espécie',
                            'Condição de Ocupação', 'Valor do Aluguel', 'Aluguel em número de salários', 'Material Predominante', 'Nº de Cômodos',
                            'Densidade de Morador', 'Cômodos  com dormitórios', 'Densidade de morador dormitório','Nº de Banheiros', 'Sanitários', 'Tipo de Esgotamento Sanitário', 'Forma de Abastecimento de Água',
                            'Canalização', 'Destino do Lixo', 'Existênia de Energia Elétrica',
                            'Existência de Medidor de Energia', 'Rádio', 'Televisão', 'Máquina de Lavar',
                            'Geladeira', 'Celular', 'Telefone Fixo', 'Microcomputador', 'Microcomputador com internet', 'Motocicleta', 'Automóvel',
                            'ALGUMA PESSOA QUE MORAVA COM VOCÊ(S) ESTAVA MORANDO EM OUTRO PAÍS EM 31 DE JULHO DE 2010',
                            'QUANTAS PESSOAS MORAVAM NESTE DOMICÍLIO EM 31 DE JULHO DE 2010', 'A RESPONSABILIDADE PELO DOMICÍLIO É DE',
                            'DE AGOSTO DE 2009 A JULHO DE 2010, FALECEU ALGUMA PESSOA QUE MORAVA COM VOCÊ(S)',
                            'Rendimento Mensal pelo domicilio em Julho de 2010',
                            'RENDIMENTO DOMICILIAR, SALÁRIOS MÍNIMOS, EM JULHO DE 2010',
                            'RENDIMENTO DOMICILIAR PER CAPITA EM JULHO DE 2010',
                            'RENDIMENTO DOMICILIAR PER CAPITA, EM Nº DE SALÁRIOS    MÍNIMOS, EM JULHO DE 2010',
                            'Espécie da Unidade Doméstica', 'ADEQUAÇÃO DA MORADIA', 'MARCA DE IMPUTAÇÃO NA V0201:',
                            'MARCA DE IMPUTAÇÃO NA V2011:', 'MARCA DE IMPUTAÇÃO NA V0202:', 'MARCA DE IMPUTAÇÃO NA V0203',
                            'MARCA DE IMPUTAÇÃO NA V0204', 'MARCA DE IMPUTAÇÃO NA V0205','MARCA DE IMPUTAÇÃO NA V0206',
                            'MARCA DE IMPUTAÇÃO NA V0207', 'MARCA DE IMPUTAÇÃO NA V0208', 'MARCA DE IMPUTAÇÃO NA V0209',
                            'MARCA DE IMPUTAÇÃO NA V0210', 'MARCA DE IMPUTAÇÃO NA V0211', 'MARCA DE IMPUTAÇÃO NA V0212',
                            'MARCA DE IMPUTAÇÃO NA V0213', 'MARCA DE IMPUTAÇÃO NA V0214', 'MARCA DE IMPUTAÇÃO NA V0215',
                            'MARCA DE IMPUTAÇÃO NA V0216', 'MARCA DE IMPUTAÇÃO NA V0217', 'MARCA DE IMPUTAÇÃO NA V0218',
                            'MARCA DE IMPUTAÇÃO NA V0219', 'MARCA DE IMPUTAÇÃO NA V0220', 'MARCA DE IMPUTAÇÃO NA V0221',
                            'MARCA DE IMPUTAÇÃO NA V0222', 'MARCA DE IMPUTAÇÃO NA V0301', 'MARCA DE IMPUTAÇÃO NA V0401',
                            'MARCA DE IMPUTAÇÃO NA V0402', 'MARCA DE IMPUTAÇÃO NA V0701', 'SITUAÇÃO DO SETOR']

            # valida os enums, caso todas as opções sejam selecionadas
        ano, estados, modalidades = self.__validar_enums(ano, estados, modalidades)

            # cria pasta temporaria no sistema
        #pasta_temporaria = self.__criar_pasta_temporaria()

            # obtem a pasta de trabalho para salvar o output (csv)
        pasta_trabalho = self.__obter_diretorio_trabalho()
        print(pasta_trabalho)
        print('Arquivos de saída salvos em {}'.format(pasta_trabalho))

            # captura o ano selecionado
        valor_ano, descricao_ano = ano.value
        print(type(valor_ano))



            # dados de documentação
        ibge_datasets, div_columns = self.__obter_dados_documentacao(pasta_trabalho, modalidades, descricao_ano)

        for arquivo_texto in arquivos:


            for enum_estado in estados:
                #try:
                valor_estado, estado, sigla = enum_estado.value
                print(type(estado))
                for enum_modalidade in modalidades:
                    valor_modalidade, descricao_modalidade = enum_modalidade.value
                    nome_arquivo_modalidade = 'Amostra_{}_{}.txt'.format(descricao_modalidade, str(valor_estado))

                    arquivo_texto = os.path.join(diretorio_textos,'Amostra_{}_{}.txt'.format(descricao_modalidade, str(valor_estado)))
                    with open(arquivo_texto, 'r', encoding='utf-8-sig') as f:

                            for chunk in pd.read_fwf(f, colspecs=div_columns[descricao_modalidade], chunksize=100000):

                                data = chunk.copy()
                                data.columns = ibge_datasets[descricao_modalidade]['VAR']
                                arquivo_csv = os.path.join(pasta_trabalho, '{}_{}.csv'.format(nome_arquivo_modalidade[:-4], sigla))
                                data.to_csv(arquivo_csv, encoding='utf-8-sig', header=header)

                    print('Arquivo de {} de modalidade {} extraído'.format(sigla, descricao_modalidade))
                    print('Arquivo de {} de modalidade {} extraído em: {}'.format(sigla, descricao_modalidade, arquivo_csv))



if __name__ == "__main__":

    ano = Anos.DEZ
    estados = [Estados.TODOS]
    modalidades = [Modalidades.TODOS]
    ibgeparser = Microdados()
    ibgeparser.obter_dados_ibge(ano, estados, modalidades)
