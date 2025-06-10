import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os
from dotenv import load_dotenv

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

pipeline_options = {
    'project': 'gcp-apachebeam',
    'runner': 'DataflowRunner',
    'region': 'southamerica-east1',
    'staging_location': 'gs://gcpapachebeamdata/temp',
    'temp_location': 'gs://gcpapachebeamdata/temp',
    'template_location': 'gs://gcpapachebeamdata/template/batch_voos'
}
pipeline_options = PipelineOptions.from_dictionary(pipeline_options)

p1 = beam.Pipeline(options=pipeline_options)

class ValidarEFiltrar(beam.DoFn):
    def process(self, record):
        try:
            if len(record) < 9:
                return
            companhia = record[4].strip()
            atraso = int(record[8].strip())
            aeroporto_origem = record[5].strip()
            if companhia == '' or aeroporto_origem == '':
                return
            if atraso < 0:
                return
            yield {
                'companhia': companhia,
                'atraso': atraso,
                'aeroporto_origem': aeroporto_origem
            }
        except Exception:
            return

dados_limpos = (
    p1
    | "Ler dados" >> beam.io.ReadFromText("gs://gcpapachebeamdata/pasta_entrada/voos_sample.csv", skip_header_lines=1)
    | "Split CSV" >> beam.Map(lambda line: line.split(','))
    | "Validar e Filtrar" >> beam.ParDo(ValidarEFiltrar())
)

media_atraso_companhia = (
    dados_limpos
    | "Map companhia-atraso" >> beam.Map(lambda d: (d['companhia'], d['atraso']))
    | "Calcular média atraso" >> beam.CombinePerKey(beam.combiners.MeanCombineFn())
)

total_voos_por_aeroporto = (
    dados_limpos
    | "Map aeroporto-total" >> beam.Map(lambda d: (d['aeroporto_origem'], 1))
    | "Contar total voos aeroporto" >> beam.CombinePerKey(sum)
)

voos_atrasados_por_aeroporto = (
    dados_limpos
    | "Filtrar voos atrasados" >> beam.Filter(lambda d: d['atraso'] > 0)
    | "Map aeroporto-atrasados" >> beam.Map(lambda d: (d['aeroporto_origem'], 1))
    | "Contar atrasos aeroporto" >> beam.CombinePerKey(sum)
)

percentual_atrasos_aeroporto = (
    {'total': total_voos_por_aeroporto, 'atrasados': voos_atrasados_por_aeroporto}
    | "CoGroupByKey atrasos/total" >> beam.CoGroupByKey()
    | "Calcular percentual" >> beam.Map(lambda kv: (
        kv[0],
        (kv[1]['atrasados'][0] if kv[1]['atrasados'] else 0) / kv[1]['total'][0] * 100
        if kv[1]['total'] else 0
    ))
)

atraso_total_por_aeroporto = (
    dados_limpos
    | "Map aeroporto-atraso" >> beam.Map(lambda d: (d['aeroporto_origem'], d['atraso']))
    | "Somar atraso total" >> beam.CombinePerKey(sum)
)

top5_aeroportos_atraso = (
    atraso_total_por_aeroporto
    | "Top 5 aeroportos atraso" >> beam.transforms.combiners.Top.Of(5, key=lambda kv: kv[1])
)

media_atraso_companhia | "Write média atraso" >> beam.io.WriteToText("gs://gcpapachebeamdata/pasta_saida/media_atraso_companhia.csv")

percentual_atrasos_aeroporto | "Write percentual atraso" >> beam.io.WriteToText("gs://gcpapachebeamdata/pasta_saida/percentual_atrasos_aeroporto.csv")

top5_aeroportos_atraso | "Write top 5 atrasos" >> beam.io.WriteToText("gs://gcpapachebeamdata/pasta_saida/top5_aeroportos_atraso.csv")

p1.run()
