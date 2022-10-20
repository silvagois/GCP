import apache_beam as beam
import os 
from config_variables import *
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = {
    'project':'data-lake-gcp',
    'runner':'DataflowRunner',
    'region':'southamerica-east1',
    'staging_location':'gs://raw-etl-gcp/temp',
    'temp_location':'gs://raw-etl-gcp/temp',
    'template_location':'gs://raw-etl-gcp/template/batch_job_df_gcs_voos'
}

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)

serviceAccount = f"{SERVICE_ACCOUNT_PATH}"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

class filtro(beam.DoFn):
  def process(self,record):
    if int(record[8]) > 0:
      return [record]

Tempo_Atrasos = (
  p1
  #| "Importar Dados Atraso" >> beam.io.ReadFromText(f"{PATH}{ARCHIVE}", skip_header_lines = 1) 
  | "Importar Dados Atraso" >> beam.io.ReadFromText(f"{PATH_ENTRADA}", skip_header_lines = 1)  
  | "Separar por Vírgulas Atraso" >> beam.Map(lambda record: record.split(','))
  | "Pegar voos com atraso" >> beam.ParDo(filtro())
  | "Criar par atraso" >> beam.Map(lambda record: (record[4],int(record[8])))
  | "Somar por key" >> beam.CombinePerKey(sum)
#  | "Mostrar Resultados" >> beam.Map(print)
)

Qtd_Atrasos = (
  p1
  | "Importar Dados" >> beam.io.ReadFromText(f"{PATH_ENTRADA}", skip_header_lines = 1)
  | "Separar por Vírgulas Qtd" >> beam.Map(lambda record: record.split(','))
  | "Pegar voos com Qtd" >> beam.ParDo(filtro())
  | "Criar par Qtd" >> beam.Map(lambda record: (record[4],int(record[8])))
  | "Contar por key" >> beam.combiners.Count.PerKey()
#  | "Mostrar Resultados QTD" >> beam.Map(print)
)

tabela_atrasos = (
    {'Qtd_Atrasos':Qtd_Atrasos,'Tempo_Atrasos':Tempo_Atrasos} 
    | "Group By" >> beam.CoGroupByKey()
    | "Saida bucket GCP" >> beam.io.WriteToText(f"{PATH_SAIDA}")
    #| beam.Map(print)
)

p1.run()