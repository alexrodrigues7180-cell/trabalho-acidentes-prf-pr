"""
Equipe: Alexandre, Laerto, Klaus e Kauan
Gera notebooks/02_pipeline_etl.ipynb (a demonstração da Etapa 2).
Uso: python src/gerar_notebook.py && jupyter nbconvert --execute --to notebook --inplace notebooks/02_pipeline_etl.ipynb
"""
from pathlib import Path
import nbformat as nbf

C = []
md = lambda s: C.append(nbf.v4.new_markdown_cell(s.strip()))
code = lambda s: C.append(nbf.v4.new_code_cell(s.strip()))

md("""
# Etapa 2 — Pipeline de Tratamento / ETL

- **Tema:** Acidentes nas rodovias federais do Paraná (2021–2025): onde, quando e por que acontecem os acidentes mais graves
- **Equipe:** Alexandre, Laerto, Klaus e Kauan
- **Fonte:** PRF — Dados Abertos, *Acidentes (agrupados por ocorrência)*, um CSV (em ZIP) por ano — <https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf>

Este notebook demonstra, passo a passo, o tratamento feito pelo pipeline `src/etl.py`.
Cada etapa é uma função do pipeline; aqui ela é chamada isoladamente para mostrar **o estado antes, a regra aplicada e o estado depois**.
O mesmo resultado é obtido de uma vez com `python src/etl.py`.

| # | Etapa | Critério da avaliação |
|---|---|---|
| 1 | Extração dos 5 anos | — |
| 2 | Padronização de texto e nomes de colunas | normalização estrutural |
| 3 | Filtragem (PR, 2021–2025) | filtragens pertinentes ao tema |
| 4 | Tratamento de nulos (incl. nulos "disfarçados") | resolução de valores nulos |
| 5 | Remoção de duplicados (3 níveis) | resolução de duplicados |
| 6 | Tipagem | tipagem correta de variáveis |
| 7 | Normalização estrutural (rótulos, campo multivalorado) | normalização estrutural |
| 8 | Variáveis derivadas para as hipóteses | filtragens pertinentes ao tema |
| 9 | Validação automática | — |
| 10 | Carga (Parquet + CSV) | — |
""")

code("""
import sys, json
from pathlib import Path
import pandas as pd
sys.path.insert(0, str(Path.cwd().parent / "src"))
import etl

pd.set_option("display.max_columns", 40)
pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 70)
etl.LOG.clear()
""")

# ------------------------------------------------------------------ 1
md("""
## 1. Extração

Os ZIPs são baixados do Google Drive da PRF (os IDs estão em `etl.DRIVE_IDS`, tirados da página oficial) para `data/raw/`.
Os CSVs usam **`;` como separador e codificação Latin-1**.

Decisão importante: tudo é lido **como texto** (`dtype=str`) e **sem conversão automática de nulos** (`keep_default_na=False`).
Assim nenhum valor é convertido "no escuro" pelo pandas: cada nulo e cada tipo é decidido de forma explícita nas etapas seguintes.
""")
code("""
etl.baixar()
bruto = etl.extrair()
print(f"{len(bruto):,} acidentes no Brasil, {bruto.shape[1]} colunas".replace(",", "."))
bruto.groupby("ano_arquivo").size().rename("acidentes (Brasil)").to_frame().T
""")
code("""
# Todas as colunas chegam como texto. Repare nos números com vírgula decimal (km, latitude, longitude).
print("Tipos:", bruto.dtypes.value_counts().to_dict())
bruto[["id", "data_inversa", "horario", "uf", "br", "km", "latitude", "longitude", "condicao_metereologica", "tracado_via"]].head(5)
""")

# ------------------------------------------------------------------ 2
md("""
## 2. Padronização de texto e nomes de colunas

- normalização Unicode (NFC) e remoção de espaços nas pontas / espaços repetidos — evita que o mesmo valor com um espaço a mais vire outra categoria (na base aparece, por exemplo, município com espaço duplo);
- nomes de colunas: `condicao_metereologica` → `condicao_meteorologica` (erro de grafia da fonte), `data_inversa` → `data`, `uso_solo` → `area_urbana` (na fonte, *Sim* = área urbana).
""")
code("""
df = etl.padronizar_texto(bruto)
mudou = pd.DataFrame({c: bruto[c] != df[etl.RENOMEAR.get(c, c)] for c in bruto.columns if bruto[c].dtype == object})
print("Células alteradas:", int(mudou.values.sum()))
exemplos = [(c, repr(v)) for c in mudou.columns for v in bruto.loc[mudou[c], c].unique()[:3]]
pd.DataFrame(exemplos, columns=["coluna", "valor original (repr)"])
""")

# ------------------------------------------------------------------ 3
md("""
## 3. Filtragem pertinente ao tema

Recorte da Etapa 1: **acidentes ocorridos no Paraná, de 2021 a 2025**.
O filtro usa a coluna `uf`, que é a **UF do local do acidente**, e não a da delegacia que atendeu:
alguns acidentes no PR (divisa) são atendidos por delegacias de SC e SP e devem continuar na base.
""")
code("""
pr = etl.filtrar(df)
print(f"Brasil: {len(df):,} -> Paraná 2021–2025: {len(pr):,} acidentes".replace(",", "."))
print("Acidentes no PR atendidos por delegacia de outra UF (mantidos):",
      int((~pr['delegacia'].str.endswith('-PR') & ~pr['delegacia'].isin(etl.SENTINELAS_NULO)).sum()))
pr.groupby("ano_arquivo").size().rename("acidentes no PR").to_frame().T
""")

# ------------------------------------------------------------------ 4
md("""
## 4. Tratamento de valores nulos

Na base da PRF quase não existe célula vazia: **o nulo vem disfarçado** de texto.
O primeiro passo é encontrar esses valores-sentinela e convertê-los em nulo de verdade:

| Sentinela | Onde aparece | Significado |
|---|---|---|
| `Ignorado` | `condicao_meteorologica` | clima não registrado |
| `Não Informado` | `sentido_via` | sentido não registrado |
| `NA`, `N/A` | `regional`, `delegacia`, `uop`, `classificacao_acidente`* | sem informação |
| `br = 0` (e `km = 0`) | `br`, `km` | não existe BR-000: rodovia não informada |

\\* `classificacao_acidente = NA` aparece 5 vezes no Brasil, nenhuma no PR — a regra fica no pipeline mesmo assim.

**Perfil de nulos do recorte PR, antes do tratamento:**
""")
code("""
etl.perfil_nulos(pr)
""")
md("""
**Regra adotada para cada caso** — o princípio foi *não inventar informação que a análise vá usar*:

| Coluna | Regra | Por quê |
|---|---|---|
| `classificacao_acidente` | recalculada a partir de `mortos`/`feridos` | é derivável sem ambiguidade |
| `regional`/`delegacia`/`uop` | copia o trio do acidente mais próximo (km) na **mesma BR e município**; sem vizinho → `"Não informado"` | a PRF atribui a unidade por trecho; coluna administrativa, não entra nas hipóteses |
| `condicao_meteorologica`, `sentido_via` | categoria explícita `"Não informado"` | imputar clima seria inventar dado justamente da hipótese 4; a linha é mantida porque as outras colunas são válidas, e esses casos saem só da análise que usa a coluna |
| `br`, `km` | mantidos como nulo + flag `localizacao_valida = False` | 70 ocorrências fora de trecho de BR; entram nas hipóteses 2–4 e saem só da hipótese 1 (trechos) |

Nenhuma linha é descartada por causa de nulo.
""")
code("""
tratado = etl.tratar_nulos(pr)
print(json.dumps(etl.LOG[-1]["regras"], ensure_ascii=False, indent=1))
""")
code("""
# Depois: o único nulo que sobra é o intencional (br/km fora da malha federal), sinalizado pela flag
restantes = tratado.isna().sum()
display(restantes[restantes > 0].rename("nulos").to_frame())
print("Linhas com localizacao_valida = False:", int((~tratado["localizacao_valida"]).sum()))
""")
code("""
# O único caso de imputação por vizinho: id 619297 (Ponta Grossa, BR-376 km 475) estava com regional/delegacia/uop = 'NA'
pd.concat([pr.loc[pr["id"] == "619297", ["id", "municipio", "br", "km", "regional", "delegacia", "uop"]].assign(estado="antes"),
           tratado.loc[tratado["id"] == "619297", ["id", "municipio", "br", "km", "regional", "delegacia", "uop"]].assign(estado="depois")])
""")
code("""
# A regra de classificação, testada nos 5 registros do Brasil que vêm com 'NA'
na_br = df[df["classificacao_acidente"] == "NA"][["id", "uf", "mortos", "feridos", "classificacao_acidente"]].copy()
m, f = na_br["mortos"].astype(int), na_br["feridos"].astype(int)
na_br["classificacao_derivada"] = ["Com Vítimas Fatais" if a > 0 else "Com Vítimas Feridas" if b > 0 else "Sem Vítimas" for a, b in zip(m, f)]
na_br
""")

# ------------------------------------------------------------------ 5
md("""
## 5. Duplicados

Verificados em três níveis:

1. **`id` repetido** — o `id` é a chave da ocorrência na PRF;
2. **linha idêntica exceto o `id`** — mesmo registro lançado duas vezes;
3. **duplicado lógico** — mesmo instante, mesmo ponto (km **e** coordenadas) e mesmas contagens de pessoas e veículos.
   Ficam de fora da chave, de propósito, campos de *percepção/registro* (clima, traçado, delegacia),
   que mudam quando dois agentes ou duas delegacias registram o mesmo acidente.

Em todos os casos fica o menor `id` (o primeiro registro). Os removidos vão para `data/processed/auditoria_duplicados_removidos.csv`.
""")
code("""
sem_dup, removidos = etl.remover_duplicados(tratado)
{k: v for k, v in etl.LOG[-1].items() if k not in ("etapa",)}
""")
md("""
Nenhum `id` repetido nem linha idêntica. A busca por **mesma data, hora, BR e km** encontra pares suspeitos,
e cada um foi analisado campo a campo antes de definir a chave final:
""")
code("""
def diferencas(a, b):
    x, y = tratado[tratado["id"] == a].iloc[0], tratado[tratado["id"] == b].iloc[0]
    return {c: f"{x[c]}  |  {y[c]}" for c in tratado.columns if x[c] != y[c] and c not in ("id",)}

pares = [("361574", "361577"), ("411196", "411197"), ("443372", "450136"), ("486992", "487025"), ("353903", "353904")]
linhas = []
for a, b in pares:
    d = diferencas(a, b)
    linhas.append({"par de ids": f"{a} / {b}", "campos que diferem": "; ".join(f"{k}: {v}" for k, v in d.items()),
                   "decisão": "REMOVIDO (duplicado)" if b in set(removidos["id"]) else "mantidos (acidentes distintos)"})
pd.DataFrame(linhas)
""")
md("""
- **361574/361577** e **411196/411197**: ids seguidos, mesmo ponto exato e mesmas contagens; só muda a percepção de clima/traçado → o mesmo acidente registrado duas vezes.
- **443372/450136**: tudo igual, mas registrado por **duas delegacias** diferentes → duplicado.
- **486992/487025** (Tibagi, chuva): coordenadas a ~7 km uma da outra e 1 × 4 ocupantes → **dois veículos diferentes** aquaplanando no mesmo trecho. Uma primeira versão da chave (sem coordenadas) removia este par por engano; a análise caso a caso corrigiu a regra.
- **353903/353904**: vítimas e coordenadas diferentes → acidentes distintos.
""")

# ------------------------------------------------------------------ 6
md("""
## 6. Tipagem

| Coluna(s) | De | Para | Observação |
|---|---|---|---|
| `id` | texto | `int64` | a fonte traz id em notação científica (`6e+05` = 600000); conversão conferida (inteiro e único) |
| `data`, `horario` | texto | `datetime64`, `time` + nova `data_hora` | combinadas para permitir análise por hora |
| `km`, `latitude`, `longitude` | texto com **vírgula** decimal | `Float64` | `"546,2"` → `546.2` |
| `br` | texto | `Int16` (aceita nulo) | |
| contagens (`mortos`, `feridos`, ...) | texto | `int16` | |
| `area_urbana` | "Sim"/"Não" | booleano | |
| `dia_semana`, `fase_dia`, `classificacao_acidente` | texto | categoria **ordenada** | permite ordenar segunda→domingo e Sem vítimas < Feridas < Fatais |
| demais textos | texto | `category` | economia de memória e domínio fechado |
""")
code("""
# O caso do id em notação científica
print(sem_dup.loc[~sem_dup["id"].str.fullmatch(r"\\d+"), ["id", "data", "horario", "municipio"]])
tipado = etl.tipar(sem_dup)
print("\\nDepois:", tipado.loc[tipado["id"] == 600000, ["id", "data", "horario", "municipio"]].to_string(index=False))
""")
code("""
cols = ["id", "data", "horario", "data_hora", "br", "km", "latitude", "mortos", "area_urbana", "dia_semana", "classificacao_acidente", "causa_acidente"]
antes_t = sem_dup.reindex(columns=cols).dtypes.astype(str)
antes_t["data_hora"] = "(não existia)"
comparacao = pd.DataFrame({"antes": antes_t, "depois": tipado[cols].dtypes.astype(str),
                           "exemplo depois": [str(tipado[c].iloc[1]) for c in cols]})
comparacao.loc[comparacao["depois"].str.startswith("category"), "depois"] = [
    "category (ordenada)" if tipado[c].cat.ordered else "category" for c in comparacao.index[comparacao["depois"].str.startswith("category")]]
comparacao
""")
code("""
mb = lambda d: d.memory_usage(deep=True).sum() / 1e6
print(f"Memória: {mb(sem_dup):.1f} MB (texto) -> {mb(tipado):.1f} MB (tipado)")
""")

# ------------------------------------------------------------------ 7
md("""
## 7. Normalização estrutural

**a) Rótulos de causa que mudaram entre os anos.** A PRF trocou a grafia de algumas causas ao longo do período.
Sem harmonizar, a mesma causa aparece como três categorias e as séries por ano ficam quebradas:
""")
code("""
afetadas = list(etl.MAPA_CAUSAS) + sorted(set(etl.MAPA_CAUSAS.values()))
pd.crosstab(tipado["causa_acidente"].astype(str), tipado["ano_arquivo"]).loc[afetadas]
""")
code("""
normal, ponte = etl.normalizar(tipado)
alvos = sorted(set(etl.MAPA_CAUSAS.values()))
display(pd.crosstab(normal["causa_acidente"].astype(str), normal["ano_arquivo"]).loc[alvos])
print("Causas distintas:", etl.LOG[-1]["causas_distintas"])
""")
md("""
**b) Campo multivalorado sem ordem: `tracado_via`.** Um acidente pode ter vários elementos de traçado, separados por `;`,
em qualquer ordem — `"Reta;Declive"` e `"Declive;Reta"` são a mesma coisa, mas contam como categorias diferentes.
Isso fere a primeira forma normal (um valor atômico por célula). O tratamento:

1. forma canônica (elementos únicos e em ordem alfabética);
2. uma coluna booleana por elemento (`tracado_curva`, `tracado_declive`, ...) para filtrar direto no dashboard;
3. tabela-ponte `acidente_tracado.csv` (uma linha por acidente × elemento), que é a forma normalizada para análise relacional (Power BI / SQL).
""")
code("""
ex = tipado["tracado_via"].astype(str)
exemplo = ex[ex.isin(["Reta;Declive", "Declive;Reta"])].value_counts()
display(exemplo.rename("antes").to_frame().assign(depois=[etl._tracado_canonico(i) for i in exemplo.index]))
print("Combinações distintas:", etl.LOG[-1]["tracado_combinacoes"])
""")
code("""
display(ponte.head(6))
normal.filter(like="tracado_").drop(columns="tracado_via").sum().sort_values(ascending=False).rename("acidentes").to_frame().T
""")
md("""
Mantido como está, e registrado como limitação: o tipo **`Colisão lateral`** só existe em 2021 (71 casos).
Depois a PRF passou a separar *mesmo sentido* e *sentido oposto*, e não há como saber qual era cada caso de 2021.
""")

# ------------------------------------------------------------------ 8
md("""
## 8. Variáveis derivadas para as hipóteses

| Variável | Regra | Hipótese |
|---|---|---|
| `fatal` | `mortos > 0` | H1–H4 |
| `grave` | `mortos > 0` ou `feridos_graves > 0` | H1–H4 |
| `trecho_10km` | BR + faixa de 10 km (ex.: `BR-277 km 50–60`); nulo se `localizacao_valida = False` | H1 — concentração em trechos |
| `noite` | `fase_dia` ∈ {Plena Noite, Anoitecer} | H2 |
| `fim_de_semana` | sábado ou domingo | H2 |
| `ano`, `mes`, `hora` | extraídos de `data_hora` | H2 / séries temporais |
| `chuva` | `condicao_meteorologica` ∈ {Chuva, Garoa/Chuvisco, Granizo} | H4 |
| `pista_molhada_proxy` | `chuva` **ou** causa ∈ {Acúmulo de água sobre o pavimento, Pista escorregadia, Chuva} | H4 |
| `total_envolvidos` | mortos + feridos + ilesos + ignorados | consistência |

**Ajuste em relação à Etapa 1:** o conjunto *agrupado por ocorrência* **não tem mais a coluna "condição da pista"**.
Para a hipótese 4 usamos o clima registrado e as causas ligadas a pista molhada (`pista_molhada_proxy`).

**Sobre `pessoas`:** em 2.650 linhas `pessoas` é menor que a soma de mortos + feridos + ilesos + ignorados.
A diferença aparece só quando há `ignorados`, ou seja, a PRF não conta em `pessoas` os envolvidos não identificados.
Não é erro a corrigir: a coluna original é mantida e `total_envolvidos` traz o total.
""")
code("""
final = etl.derivar(normal)
resumo = pd.Series({
    "acidentes": len(final), "fatais": int(final["fatal"].sum()), "graves (fatal ou ferido grave)": int(final["grave"].sum()),
    "à noite": int(final["noite"].sum()), "no fim de semana": int(final["fim_de_semana"].sum()),
    "com chuva": int(final["chuva"].sum()), "pista molhada (proxy)": int(final["pista_molhada_proxy"].sum()),
    "trechos de 10 km distintos": final["trecho_10km"].nunique()}).rename("quantidade").to_frame()
resumo
""")
code("""
print("pessoas != soma das categorias:", int((final["pessoas"] != final["total_envolvidos"]).sum()))
print("...e destes, quantos têm ignorados > 0:", int(((final["pessoas"] != final["total_envolvidos"]) & (final["ignorados"] > 0)).sum()))
""")

# ------------------------------------------------------------------ 9
md("""
## 9. Validação automática

Regras que a base tratada tem de cumprir. Se alguma falhar, o pipeline para com erro — nada é gravado com problema.
(Foi esta etapa que pegou o `uop = "N/A"` que escapou da primeira versão do tratamento de nulos.)
""")
code("""
checks = etl.validar(final)
pd.Series(checks).map({True: "OK", False: "FALHOU"}).rename("resultado").to_frame()
""")

# ------------------------------------------------------------------ 10
md("""
## 10. Carga

| Arquivo | Conteúdo |
|---|---|
| `acidentes_pr_2021_2025.parquet` | base tratada com os tipos preservados (para Python/Streamlit) |
| `acidentes_pr_2021_2025.csv` | mesma base, UTF-8, ponto decimal (para Power BI/Excel) |
| `acidente_tracado.csv` | tabela-ponte acidente × elemento do traçado |
| `auditoria_duplicados_removidos.csv` | registros removidos e o motivo |
| `log_etl.json` | linhas antes/depois e regras de cada etapa |
""")
code("""
salvo = etl.carregar(final, ponte, removidos)
print(f"Base final: {salvo.shape[0]:,} acidentes x {salvo.shape[1]} colunas".replace(",", "."))
funil = pd.DataFrame(etl.LOG)[["etapa", "linhas_antes", "linhas_depois"]]
funil["variação"] = funil["linhas_depois"] - funil["linhas_antes"]
funil["variação"] = funil["variação"].astype("Int64")
funil.loc[0, "variação"] = pd.NA
funil
""")
code("""
# Conferência: o arquivo gravado volta com os mesmos tipos e sem nulos fora do esperado
relido = pd.read_parquet(etl.PROCESSED / "acidentes_pr_2021_2025.parquet")
print(relido.shape, "| nulos:", relido.isna().sum()[lambda s: s > 0].to_dict())
relido.head(3)
""")

md("""
## Resumo das decisões

- **Recorte:** 342.670 acidentes no Brasil → **37.062 no PR (2021–2025)** → **37.059** após remover 3 duplicados.
- **Nulos:** 641 nulos disfarçados encontrados (`Ignorado`, `Não Informado`, `NA`, `N/A`, `br = 0`). Nenhuma linha descartada; cada coluna com regra própria. Sobram só os 70 `br/km` fora da malha federal, sinalizados por flag.
- **Duplicados:** nenhum exato; 3 duplicados lógicos removidos depois de analisar caso a caso. Um par que parecia duplicado era, na verdade, de dois acidentes diferentes (Tibagi).
- **Tipagem:** de 30 colunas de texto para tipos numéricos, datas, booleanos e categorias ordenadas. Inclui a correção da vírgula decimal e do id em notação científica.
- **Normalização:** nomes de colunas corrigidos; 76 → 73 causas (grafias harmonizadas); `tracado_via` de 395 → 166 combinações, com colunas booleanas e tabela-ponte.
- **Pronto para a Etapa 3:** variáveis `fatal`, `grave`, `trecho_10km`, `noite`, `fim_de_semana`, `chuva` e `pista_molhada_proxy`, ligadas às hipóteses H1–H4.

**Limitações registradas:** a fonte não tem mais "condição da pista" (usamos proxy); `Colisão lateral` só em 2021; `pessoas` não conta os envolvidos ignorados.
""")

nb = nbf.v4.new_notebook()
nb["cells"] = C
nb["metadata"]["kernelspec"] = {"name": "python3", "display_name": "Python 3", "language": "python"}
nb["metadata"]["language_info"] = {"name": "python"}
saida = Path(__file__).resolve().parents[1] / "notebooks" / "02_pipeline_etl.ipynb"
nbf.write(nb, saida)
print("ok", saida)
