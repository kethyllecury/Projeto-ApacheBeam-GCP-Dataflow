# Análise de Atrasos em Voos com Apache Beam e Dataflow

## Descrição

Este projeto implementa um pipeline de processamento de dados em Apache Beam para análise de atrasos em voos, utilizando o Google Cloud Dataflow como executor. O pipeline consome dados históricos de voos em formato CSV armazenados no Cloud Storage, realiza validação e limpeza, e gera métricas importantes para monitoramento do desempenho de companhias aéreas e aeroportos.

## Objetivo

Garantir a qualidade dos dados de voos considerando apenas registros válidos (companhia e aeroporto de origem informados, atraso maior ou igual a zero) e calcular indicadores que suportem a otimização das operações aéreas, como:

- Média de atraso por companhia aérea  
- Total e percentual de voos atrasados por aeroporto  
- Ranking dos 5 aeroportos com maior atraso acumulado

## Regra de Negócio

- Os dados de voos devem ser validados para garantir qualidade, considerando somente voos com companhia e aeroporto de origem informados, e com atraso igual ou superior a zero.  
- Com os dados limpos, a empresa monitora indicadores de desempenho para melhorar a gestão operacional.

## Funcionamento do Pipeline

1. **Leitura dos dados**: Os dados são lidos de um arquivo CSV no Cloud Storage.  
2. **Validação e filtragem**: São removidos registros com menos de 9 campos, atrasos negativos ou campos obrigatórios vazios.  
3. **Cálculos realizados**:  
   - Média de atraso por companhia aérea  
   - Total e percentual de voos atrasados por aeroporto  
   - Top 5 aeroportos com maior atraso acumulado  
4. **Escrita dos resultados**: Os resultados são gravados em arquivos no Cloud Storage para posterior análise e visualização.

## Componentes Utilizados

| Componente             | Descrição                                                    |
|-----------------------|--------------------------------------------------------------|
| Cloud Storage (entrada) | Armazenamento dos dados brutos dos voos (CSV)               |
| Apache Beam            | Framework para criação do pipeline de processamento          |
| Google Cloud Dataflow  | Serviço gerenciado que executa o pipeline de forma escalável |
| Cloud Storage (saída)  | Armazenamento dos arquivos gerados com as métricas           |


