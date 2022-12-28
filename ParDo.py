import apache_beam as beam
import pyarrow as pa

p1 = beam.Pipeline()

schema = pa.schema([
  pa.field(('data'),pa.date64()),
  pa.field(('voo'),pa.string()),
  pa.field(('numero'),pa.int64()),
  pa.field(('sigla_aeroporto_saida'),pa.string()),
  pa.field(('col6'),pa.string()),
  pa.field(('col7'),pa.string()),
  pa.field(('col8'),pa.string()),
  pa.field(('col9'),pa.string()),
  pa.field(('col10'),pa.string()),
  pa.field(('col11'),pa.string()),
  pa.field(('col12'),pa.string()) 
])


class filtro(beam.DoFn):
  def process(self,record):
    if int(record[8]) > 0:
      return [record]

Tempo_Atrasos = (
  p1
  | "Importar Dados Atraso" >> beam.io.ReadFromText("voos_sample.csv", skip_header_lines = 1)  
  #| "Importar Dados Atraso" >> beam.io.ReadFromText(r"C:\Users\cassi\Google Drive\GCP\Dataflow Course\Meu_Curso\voos_sample.csv", skip_header_lines = 1)
  | "Separar por Vírgulas Atraso" >> beam.Map(lambda record: record.split(','))
  | "Pegar voos com atraso" >> beam.ParDo(filtro()) # Foi substituido essa linha pela função 
  | "Criar par atraso" >> beam.Map(lambda record: (record[4],int(record[8])))
  | "Somar por key" >> beam.CombinePerKey(sum)
#  | "Mostrar Resultados" >> beam.Map(print)
)

Qtd_Atrasos = (
  p1
  | "Importar Dados" >> beam.io.ReadFromText("voos_sample.csv", skip_header_lines = 1)
  #| "Importar Dados" >> beam.io.ReadFromText(r"C:\Users\cassi\Google Drive\GCP\Dataflow Course\Meu_Curso\voos_sample.csv", skip_header_lines = 1)
  | "Separar por Vírgulas Qtd" >> beam.Map(lambda record: record.split(','))
  | "Pegar voos com Qtd" >> beam.ParDo(filtro()) # Foi substituido essa linha pela função 
  | "Criar par Qtd" >> beam.Map(lambda record: (record[4],int(record[8])))
  | "Contar por key" >> beam.combiners.Count.PerKey()
#  | "Mostrar Resultados QTD" >> beam.Map(print)
)

tabela_atrasos = (
    {'Qtd_Atrasos':Qtd_Atrasos,'Tempo_Atrasos':Tempo_Atrasos} 
    | "Group By" >> beam.CoGroupByKey()
    #| "Saida GCP Bucket" >> beam.io.WriteToText("gs://raw-etl-gcp/entrada/voos_agrupado.csv")
    #| "Saida GCP Bucket" >> beam.io.WriteToParquet("gs://raw-etl-gcp/entrada/voos_agrupado.parquet", schema=schema,codec='snappy', file_name_suffix='.parquet')

    | beam.Map(print)
)

p1.run()



