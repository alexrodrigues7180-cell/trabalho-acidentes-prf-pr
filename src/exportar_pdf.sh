#!/usr/bin/env bash
# Executa o notebook e exporta HTML + PDF (impressão do Chrome em modo headless).
# Equipe: Alexandre, Laerto, Klaus e Kauan
set -euo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin
$PY/python src/gerar_notebook.py
$PY/jupyter nbconvert --execute --to notebook --inplace notebooks/02_pipeline_etl.ipynb
$PY/jupyter nbconvert --to html notebooks/02_pipeline_etl.ipynb --output-dir docs
CSS='<style>@page{size:A4 landscape;margin:12mm}
.jp-InputPrompt,.jp-OutputPrompt{display:none!important}
pre,code{white-space:pre-wrap!important;word-break:break-word}
.jp-OutputArea-output{overflow:visible!important}
table{font-size:10px!important} .jp-RenderedHTMLCommon td,.jp-RenderedHTMLCommon th{padding:2px 5px!important}
.jp-Cell{break-inside:auto}</style>'
python3 - "$CSS" <<'PY'
import sys; p='docs/02_pipeline_etl.html'; s=open(p,encoding='utf-8').read()
open(p,'w',encoding='utf-8').write(s.replace('</head>', sys.argv[1]+'</head>',1))
PY
google-chrome --headless=new --disable-gpu --no-pdf-header-footer \
  --print-to-pdf="docs/Etapa2_Pipeline_ETL.pdf" "file://$PWD/docs/02_pipeline_etl.html"
