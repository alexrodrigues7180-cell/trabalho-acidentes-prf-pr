# Acidentes nas rodovias federais do Paraná (2021–2025)

Projeto Prático do 2º Bimestre — Análise de Dados Governamentais Abertos.
Equipe: Alexandre, Laerto, Klaus e Kauan.

**Pergunta:** onde, quando e por que acontecem os acidentes mais graves nas BRs do Paraná?

## Fonte
PRF — Dados Abertos, *Acidentes (agrupados por ocorrência)*, 2021 a 2025, CSV em ZIP, atualização mensal.
<https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf>

## Como reproduzir
```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python src/etl.py          # baixa os 5 ZIPs, trata e grava em data/processed/
src/exportar_pdf.sh                  # (opcional) executa o notebook e gera docs/Etapa2_Pipeline_ETL.pdf
```

## Estrutura
| Caminho | Conteúdo |
|---|---|
| `src/etl.py` | pipeline: extração → padronização → filtro PR → nulos → duplicados → tipagem → normalização → derivadas → validação → carga |
| `notebooks/02_pipeline_etl.ipynb` | demonstração da Etapa 2, etapa por etapa (antes / regra / depois) |
| `data/processed/acidentes_pr_2021_2025.parquet` | base tratada (37.059 acidentes × 55 colunas, tipos preservados) |
| `data/processed/acidente_tracado.csv` | tabela-ponte acidente × elemento do traçado da via |
| `data/processed/auditoria_duplicados_removidos.csv` | registros removidos e o motivo |
| `data/processed/log_etl.json` | linhas antes/depois e regras aplicadas em cada etapa |

## Hipóteses (Etapa 1)
1. Acidentes com morte se concentram em trechos específicos (BR + km) — `trecho_10km`, `fatal`
2. À noite e no fim de semana a gravidade é maior — `noite`, `fim_de_semana`, `grave`
3. Algumas causas têm letalidade muito acima da média — `causa_acidente`, `fatal`
4. Chuva / pista molhada aumentam a proporção de graves — `chuva`, `pista_molhada_proxy`
