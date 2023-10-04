from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime
import json
import requests
from datetime import datetime, timedelta
from pytz import timezone
from xml.etree import ElementTree as ET
from confluent_kafka.avro import AvroProducer
from confluent_kafka import Producer
from confluent_kafka import avro


# Funcao para extrair os dados da senior
def extrai_senior(user, password, encryption, url, offset):

    xml_data = f'''
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ser="http://services.senior.com.br">
        <soapenv:Header/>
        <soapenv:Body>
            <ser:Relacao_Colaboradores>
                <user>{user}</user>
                <password>{password}</password>
                <encryption>{encryption}</encryption>
                <parameters>
                    <offset>{offset}</offset>
                </parameters>
            </ser:Relacao_Colaboradores>
        </soapenv:Body>
    </soapenv:Envelope>
    '''

    # Criar cliente SOAP
    # client = Client(url)
    headers = {
        'Content-Type': 'text/xml; charset=utf-8',
        #'SOAPAction': 'http://www.example.com/webservice/SeuMetodo'
    }

    # Envie a requisição POST com o XML 
    response = requests.post(url, data=xml_data, headers=headers, verify=False)

    # Verifique se a requisição foi bem-sucedida
    if response.status_code == 200:
        # Analise a resposta XML
        xml_response = ET.fromstring(response.content)
    
        
        saida_elements = xml_response.findall('.//saida')
        # Encontre o elemento 'RelacaoColaboradoresResponse'
        if not saida_elements:
            return  


        # Inicialize uma lista para armazenar os dados extraídos
        resultado = []

        # Itere sobre os elementos de 'saida' e extraia os valores
        for elemento in saida_elements:
            cadGes = elemento.find('.//cadGes').text
            cidNas = elemento.find('.//cidNas').text
            codCar = elemento.find('.//codCar').text
            codCcu = elemento.find('.//codCcu').text
            codEst = elemento.find('.//codEst').text
            codFil = elemento.find('.//codFil').text
            codVin = elemento.find('.//codVin').text
            datAdm = elemento.find('.//datAdm').text
            datAfa = elemento.find('.//datAfa').text
            datNas = elemento.find('.//datNas').text
            defFis = elemento.find('.//defFis').text
            desGra = elemento.find('.//desGra').text
            desNac = elemento.find('.//desNac').text
            desSit = elemento.find('.//desSit').text
            desVin = elemento.find('.//desVin').text
            emaCom = elemento.find('.//emaCom').text
            empGes = elemento.find('.//empGes').text
            filGes = elemento.find('.//filGes').text
            horMes = elemento.find('.//horMes').text
            nomCcu = elemento.find('.//nomCcu').text
            nomFil = elemento.find('.//nomFil').text
            nomFun = elemento.find('.//nomFun').text
            nomGes = elemento.find('.//nomGes').text
            numCad = elemento.find('.//numCad').text
            numCpf = elemento.find('.//numCpf').text
            numEmp = elemento.find('.//numEmp').text
            sitAfa = elemento.find('.//sitAfa').text
            tipCol = elemento.find('.//tipCol').text
            tipSex = elemento.find('.//tipSex').text
            titCar = elemento.find('.//titCar').text
            titRed = elemento.find('.//titRed').text

            # Crie um dicionário com os valores e adicione à lista de resultado
            dados = {
                'cadGes': cadGes,
                'cidNas': cidNas,
                'codCar': codCar,
                'codCcu': codCcu,
                'codEst': codEst,
                'codFil': codFil,
                'codVin': codVin,
                'datAdm': datAdm,
                'datAfa': datAfa,
                'datNas': datNas,
                'defFis': defFis,
                'desGra': desGra,
                'desNac': desNac,
                'desSit': desSit,
                'desVin': desVin,
                'emaCom': emaCom,
                'empGes': empGes,
                'filGes': filGes,
                'horMes': horMes,
                'nomCcu': nomCcu,
                'nomFil': nomFil,
                'nomFun': nomFun,
                'nomGes': nomGes,
                'numCad': numCad,
                'numCpf': numCpf,
                'numEmp': numEmp,
                'sitAfa': sitAfa,
                'tipCol': tipCol,
                'tipSex': tipSex,
                'titCar': titCar,
                'titRed': titRed,
            }

            resultado.append(dados)

        # Converta a lista de resultado para JSON
        resultado_json = json.dumps(resultado, indent=4)

        # Imprima o resultado em formato JSON
        print(f'offset =  {offset}')
        
        # with open(nome_arquivo, 'a') as arquivo:
        #     arquivo.write(resultado_json)

        # Aumente o offset em 5000 para a próxima página
        # offset += 5000
        return resultado_json
    else:
        print(f'Erro na requisição HTTP. Código de status: {response.status_code}')


# Funcao que envia os dados para um topico do Kafka
def envia_kafka_topico():

    # Pegando os dados da conexao
    conn = BaseHook.get_connection("conn_senior")
    data = json.loads(conn.get_extra())

    #Configuracoes usadas para extrair os dados e um produtor, se o produtor for sem schema descomento o correspondente
    # conf_producer = {
    #     'bootstrap.servers': 'kafkacluster-kafka-bootstrap.kafka.svc.cluster.local:9094',
    #     'schema.registry.url': 'http://schema-registry-cp-schema-registry.kafka.svc.cluster.local:8081',
    #     'client.id': 'testeavro'
    # }

    conf_producer = {
        'bootstrap.servers': '20.84.11.171:9094',
        'client.id': 'testeavro'
    }

    offset = 0

    # Schema usado para o topico
    schema = """{
        "type": "record",
        "name": "Funcionario",
        "fields": [
            {"name": "cadGes", "type": "string"},
            {"name": "cidNas", "type": "string"},
            {"name": "codCar", "type": "string"},
            {"name": "codCcu", "type": "string"},
            {"name": "codEst", "type": "string"},
            {"name": "codFil", "type": "string"},
            {"name": "codVin", "type": "string"},
            {"name": "datAdm", "type": "string"},
            {"name": "datAfa", "type": "string"},
            {"name": "datNas", "type": "string"},
            {"name": "defFis", "type": "string"},
            {"name": "desGra", "type": "string"},
            {"name": "desNac", "type": "string"},
            {"name": "desSit", "type": "string"},
            {"name": "desVin", "type": "string"},
            {"name": "emaCom", "type": "string"},
            {"name": "empGes", "type": "string"},
            {"name": "filGes", "type": "string"},
            {"name": "horMes", "type": "string"},
            {"name": "nomCcu", "type": "string"},
            {"name": "nomFil", "type": "string"},
            {"name": "nomFun", "type": "string"},
            {"name": "nomGes", "type": "string"},
            {"name": "numCad", "type": "string"},
            {"name": "numCpf", "type": "string"},
            {"name": "numEmp", "type": "string"},
            {"name": "sitAfa", "type": "string"},
            {"name": "tipCol", "type": "string"},
            {"name": "tipSex", "type": "string"},
            {"name": "titCar", "type": "string"},
            {"name": "titRed", "type": "string"}
        ]
    }"""

    while True:
        # Extraindo os dados
        try:
            result = extrai_senior(data["user"], data["password"], data["encryption"], data["url"], offset)
        except Exception as e:
            print(f"Erro: {e}")
            break 

        # Enviando os dados extraidos ao topico
        try:

            # Para usar um producer comum ou com avro, comente e descomente a linha abaixo e as linhas das configuracoes
            # producer = AvroProducer(conf_producer, default_value_schema=avro.loads(schema))
            producer = Producer(conf_producer)
            
            for value in json.loads(result):

                output_dict = {key: str(value) if value is not None else "None" for key, value in value.items()}
                
                # Para produzir com schema avro não use o json.dumps, envie o dicionario direto
                producer.produce(topic="src-senior-relacao-colaboradores-json", value=json.dumps(output_dict))
                print(f"Produced")

            producer.flush()
        except Exception as e:
            print(f"Erro: {e}")
            break
    
        offset += 5000




default_args = {
  'owner': 'SALESFORCE-INGESTION',
  'depends_on_past': False,
  'retries': 3,
  'retry_delay': timedelta(minutes=5)
}

# declare dag
dag = DAG(
    'SENIOR-RELACAO-COLABORADORES-INGEST',
    default_args=default_args,
    start_date= datetime(2023, 8, 25, 6, 0, tzinfo=timezone('America/Sao_Paulo')),
    schedule_interval= None,#'0 6-22/2 * * *',
    tags=['Kafka', 'bronze','senior'],
    catchup=False,
    max_active_runs=1
)

extract_transform_task = PythonOperator(
    task_id='extract_transform_data',
    python_callable=envia_kafka_topico,
    provide_context=True,
    dag=dag
)

extract_transform_task