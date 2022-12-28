import apache_beam as beam
import pyarrow as pa

p1 = beam.Pipeline()


schema = pa.schema([
  ('data',pa.date64()),
  ('voo',pa.string()),
  ('numero',pa.int64()),
  ('sigla_aeroporto_saida',pa.string()),
  ('col6',pa.string()),
  ('col7',pa.string()),
  ('col8',pa.string()),
  ('col9',pa.string()),
  ('col10',pa.string()),
  ('col11',pa.string()),
  ('col12',pa.string()) 
])

# skip_header_lines = 1
Tempo_Atrasos = (
  p1
  | "Importar Dados Atraso" >> beam.io.ReadFromText("voos_sample.csv",skip_header_lines = 1 )  
  #| "Importar Dados Atraso" >> beam.io.ReadFromText(r"C:\Users\cassi\Google Drive\GCP\Dataflow Course\Meu_Curso\voos_sample.csv", skip_header_lines = 1)
  #| "Separar por Vírgulas Atraso" >> beam.Map(lambda record: record.split(','))  
  #| "Criar par atraso" >> beam.Map(lambda record: (record[4],int(record[8])))
  #| "Somar por key" >> beam.CombinePerKey(sum)
  #| "Mostrar Resultados" >> beam.Map(print)
  | "Salvando GCP Bucket Parquet" >> beam.io.WriteToParquet(file_path_prefix="gs://raw-etl-gcp/entrada/", schema=schema,codec='snappy', file_name_suffix='.parquet')
)
p1.run ()