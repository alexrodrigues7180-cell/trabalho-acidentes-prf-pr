"""
Equipe: Alexandre, Laerto, Klaus e Kauan

Pipeline de Tratamento / ETL — Acidentes nas rodovias federais do Paraná (2021–2025)
Fonte: PRF, Dados Abertos — "Acidentes (agrupados por ocorrência)", um ZIP/CSV por ano.

Uso:
    python src/etl.py            # baixa (se faltar), trata e grava em data/processed/

Cada etapa é uma função pura (DataFrame -> DataFrame) que registra o que fez em LOG,
para que o notebook de demonstração possa chamá-las uma a uma e mostrar o antes/depois.
"""
from __future__ import annotations

import hashlib
import json
import unicodedata
import urllib.request
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
RAW = RAIZ / "data" / "raw"
PROCESSED = RAIZ / "data" / "processed"

ANOS = range(2021, 2026)
UF = "PR"

# IDs dos arquivos no Google Drive, tirados da página oficial da PRF
# (https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf),
# linha "Documento CSV de Acidentes AAAA (Agrupados por ocorrência)".
DRIVE_IDS = {
    2021: "12xH8LX9aN2gObR766YN3cMcuycwyCJDz",
    2022: "1PRQjuV5gOn_nn6UNvaJyVURDIfbSAK4-",
    2023: "1-WO3SfNrwwZ5_l7fRTiwBKRw7mi1-HUq",
    2024: "14lB0vqMFkaZj8HZ44b0njYgxs9nAN8KO",
    2025: "1-G3MdmHBt6CprDwcW99xxC4BZ2DU5ryR",
}

# Valores que a PRF usa no lugar de "sem informação". Viram NaN de verdade.
SENTINELAS_NULO = {"", "NA", "N/A", "(null)", "NULL", "Ignorado", "Não Informado", "Nao Informado"}

# Rótulos de causa que mudaram de grafia entre os anos -> rótulo único.
MAPA_CAUSAS = {
    "Transitar no acostamento": "Transitar no Acostamento",
    "Ingestão de álcool e/ou substâncias psicoativas pelo pedestre": "Pedestre - Ingestão de álcool/ substâncias psicoativas",
    "Ingestão de álcool ou de substâncias psicoativas pelo pedestre": "Pedestre - Ingestão de álcool/ substâncias psicoativas",
}

RENOMEAR = {
    "data_inversa": "data",
    "condicao_metereologica": "condicao_meteorologica",  # corrige o erro de grafia da fonte
    "uso_solo": "area_urbana",
}

ORDEM_DIAS = ["segunda-feira", "terça-feira", "quarta-feira", "quinta-feira", "sexta-feira", "sábado", "domingo"]
ORDEM_FASE = ["Amanhecer", "Pleno dia", "Anoitecer", "Plena Noite"]
ORDEM_CLASSIF = ["Sem Vítimas", "Com Vítimas Feridas", "Com Vítimas Fatais"]

COLS_CONTAGEM = ["pessoas", "mortos", "feridos_leves", "feridos_graves", "ilesos", "ignorados", "feridos", "veiculos"]
COLS_CATEGORICAS = [
    "municipio", "causa_acidente", "tipo_acidente", "sentido_via", "condicao_meteorologica",
    "tipo_pista", "tracado_via", "regional", "delegacia", "uop",
]

CHUVA = {"Chuva", "Garoa/Chuvisco", "Granizo"}
CAUSAS_PISTA_MOLHADA = {"Acumulo de água sobre o pavimento", "Pista Escorregadia", "Chuva"}

# Limites aproximados do Paraná (com folga) para validar coordenadas.
BBOX_PR = {"lat": (-26.8, -22.4), "lon": (-54.7, -47.9)}

LOG: list[dict] = []


def registrar(etapa: str, antes: int, depois: int, **detalhes):
    LOG.append({"etapa": etapa, "linhas_antes": antes, "linhas_depois": depois, **detalhes})


# --------------------------------------------------------------------------- EXTRAÇÃO
def baixar(anos=ANOS, destino: Path = RAW) -> list[Path]:
    """Baixa os ZIPs que ainda não existem em data/raw/."""
    destino.mkdir(parents=True, exist_ok=True)
    caminhos = []
    for ano in anos:
        alvo = destino / f"datatran{ano}.zip"
        if not alvo.exists():
            url = f"https://drive.usercontent.google.com/download?id={DRIVE_IDS[ano]}&export=download&confirm=t"
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=300) as r:
                alvo.write_bytes(r.read())
        if not zipfile.is_zipfile(alvo):
            raise RuntimeError(f"{alvo} não é um ZIP válido (o Drive pode ter devolvido uma página HTML).")
        caminhos.append(alvo)
    return caminhos


def sha256(caminho: Path) -> str:
    return hashlib.sha256(caminho.read_bytes()).hexdigest()


def extrair(anos=ANOS, origem: Path = RAW) -> pd.DataFrame:
    """Lê todos os anos como TEXTO puro (dtype=str, sem conversão automática de nulos),
    para que cada decisão de tipagem e de nulo seja explícita nas etapas seguintes."""
    partes = []
    for ano in anos:
        with zipfile.ZipFile(origem / f"datatran{ano}.zip") as z:
            nome = z.namelist()[0]
            with z.open(nome) as f:
                d = pd.read_csv(f, sep=";", encoding="latin-1", dtype=str, keep_default_na=False)
        d["ano_arquivo"] = ano
        partes.append(d)
    df = pd.concat(partes, ignore_index=True)
    registrar("1. Extração (5 CSVs, Brasil inteiro)", 0, len(df),
              arquivos={a: sha256(origem / f"datatran{a}.zip")[:16] for a in anos})
    return df


# --------------------------------------------------------------------------- TRANSFORMAÇÃO
def padronizar_texto(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza Unicode (NFC), tira espaços nas pontas e espaços repetidos,
    e padroniza os nomes de colunas (corrige 'metereologica', 'data_inversa' -> 'data')."""
    df = df.rename(columns=RENOMEAR).copy()
    alterados = 0
    for c in df.columns:
        if df[c].dtype == object:
            novo = (df[c].map(lambda s: unicodedata.normalize("NFC", s))
                          .str.strip().str.replace(r"\s+", " ", regex=True))
            alterados += int((novo != df[c]).sum())
            df[c] = novo
    registrar("2. Padronização de texto e nomes", len(df), len(df), celulas_alteradas=alterados, colunas_renomeadas=RENOMEAR)
    return df


def filtrar(df: pd.DataFrame, uf: str = UF, anos=ANOS) -> pd.DataFrame:
    """Recorte do tema: acidentes ocorridos no PR (uf do local do acidente) entre 2021 e 2025."""
    antes = len(df)
    datas = pd.to_datetime(df["data"], format="%Y-%m-%d", errors="coerce")
    m = (df["uf"] == uf) & datas.dt.year.isin(list(anos))
    out = df.loc[m].drop(columns=["uf"]).reset_index(drop=True)
    registrar("3. Filtragem: uf = PR e 2021–2025", antes, len(out),
              removidas_outras_ufs=int((df["uf"] != uf).sum()),
              removidas_fora_periodo=int(((df["uf"] == uf) & ~datas.dt.year.isin(list(anos))).sum()))
    return out


def perfil_nulos(df: pd.DataFrame) -> pd.DataFrame:
    """Conta, por coluna, nulos reais e valores-sentinela que significam 'sem informação'."""
    linhas = []
    for c in df.columns:
        s = df[c]
        reais = int(s.isna().sum())
        if s.dtype == object:
            sent = s.isin(SENTINELAS_NULO)
            detalhe = s[sent].value_counts().to_dict()
        else:
            sent, detalhe = pd.Series(False, index=s.index), {}
        extra = {}
        if c == "br":
            extra = {"0": int((s.astype(str) == "0").sum())}
        total = reais + int(sent.sum()) + sum(extra.values())
        if total:
            linhas.append({"coluna": c, "nulos_reais": reais, "sentinelas": {**detalhe, **extra},
                           "total": total, "pct": round(100 * total / len(df), 2)})
    return pd.DataFrame(linhas)


def tratar_nulos(df: pd.DataFrame) -> pd.DataFrame:
    """1) converte sentinelas em NaN; 2) resolve cada coluna com uma regra explícita."""
    df = df.copy()
    antes_perfil = perfil_nulos(df)

    # 1) sentinelas -> NaN
    for c in df.columns:
        if df[c].dtype == object:
            df.loc[df[c].isin(SENTINELAS_NULO), c] = np.nan
    # br = 0 não existe (não há BR-000): é "BR não informada". km = 0 nessas linhas também é ausência.
    sem_br = df["br"] == "0"
    df.loc[sem_br, "br"] = np.nan
    df.loc[sem_br & (df["km"] == "0"), "km"] = np.nan

    regras = {}

    # 2a) classificacao_acidente: é derivável das contagens -> recalcula onde faltar.
    falta = df["classificacao_acidente"].isna()
    m = pd.to_numeric(df["mortos"]); f = pd.to_numeric(df["feridos"])
    derivada = np.select([m > 0, f > 0], ["Com Vítimas Fatais", "Com Vítimas Feridas"], "Sem Vítimas")
    df.loc[falta, "classificacao_acidente"] = derivada[falta.to_numpy()]
    regras["classificacao_acidente"] = f"{int(falta.sum())} derivadas de mortos/feridos"

    # 2b) regional/delegacia/uop: a PRF atribui a unidade por trecho de rodovia. Se existe acidente
    #     na mesma BR e município, copia o trio do mais próximo em km. Sem vizinho (ex.: município
    #     com 1 único registro e BR ausente), não há base para inferir -> "Não informado".
    #     São colunas administrativas, nenhuma hipótese depende delas.
    org = ["regional", "delegacia", "uop"]
    falta = df[org].isna().any(axis=1)
    completos = df[~falta]
    km_num = pd.to_numeric(df["km"].str.replace(",", ".", regex=False), errors="coerce")
    por_vizinho = 0
    for i in df.index[falta]:
        viz = completos[(completos["br"] == df.at[i, "br"]) & (completos["municipio"] == df.at[i, "municipio"])]
        if len(viz):
            j = (km_num[viz.index] - km_num[i]).abs().idxmin()
            df.loc[i, org] = df.loc[j, org].to_numpy()
            por_vizinho += 1
    sobra = int(df[org].isna().sum().sum())
    df[org] = df[org].fillna("Não informado")
    regras["regional/delegacia/uop"] = (f"{int(falta.sum())} linha(s): {por_vizinho} pelo acidente mais próximo "
                                        f"na mesma BR/município, {sobra} célula(s) -> 'Não informado'")

    # 2c) categorias descritivas sem informação: NÃO se inventa clima/sentido.
    #     A linha fica (as outras colunas são válidas) e o vazio vira a categoria explícita "Não informado".
    for c in ["condicao_meteorologica", "sentido_via"]:
        n = int(df[c].isna().sum())
        df[c] = df[c].fillna("Não informado")
        regras[c] = f"{n} -> categoria 'Não informado' (excluídas só na hipótese que usa a coluna)"

    # 2d) br/km ausentes: mantidos como NaN + flag; ficam fora só da hipótese 1 (trechos).
    df["localizacao_valida"] = df["br"].notna() & df["km"].notna()
    regras["br/km"] = f"{int((~df['localizacao_valida']).sum())} mantidos como nulo + flag localizacao_valida=False"

    registrar("4. Tratamento de nulos", len(df), len(df),
              nulos_antes=int(antes_perfil["total"].sum()) if len(antes_perfil) else 0,
              regras=regras)
    return df


# Mesmo instante, mesmo ponto (km E coordenadas) e mesmas contagens de pessoas/veículos.
# Ficam de FORA de propósito: clima, traçado e delegacia — são campos de percepção/registro
# que variam quando dois agentes (ou duas delegacias) registram o mesmo acidente.
CHAVE_DUP_LOGICO = ["data", "horario", "br", "km", "municipio", "sentido_via", "tipo_acidente",
                    "causa_acidente", "latitude", "longitude", "pessoas", "mortos",
                    "feridos_leves", "feridos_graves", "ilesos", "ignorados", "veiculos"]


def remover_duplicados(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Três níveis: (a) id repetido; (b) linha idêntica exceto id;
    (c) duplicado lógico — ver CHAVE_DUP_LOGICO: é o mesmo acidente registrado duas vezes.
    Mantém o menor id. Devolve (df_limpo, removidos) para auditoria."""
    antes = len(df)
    ordenado = df.sort_values("id", key=lambda s: pd.to_numeric(s))
    a = ordenado.duplicated("id")
    sem_id = [c for c in df.columns if c not in ("id", "ano_arquivo")]
    b = ordenado.duplicated(sem_id) & ~a
    c = ordenado.duplicated(CHAVE_DUP_LOGICO) & ~a & ~b
    removidos = ordenado[a | b | c].assign(motivo=np.select([a, b, c], ["id repetido", "linha idêntica", "duplicado lógico"], "")[(a | b | c).to_numpy()])
    out = ordenado[~(a | b | c)].sort_index().reset_index(drop=True)
    registrar("5. Remoção de duplicados", antes, len(out),
              id_repetido=int(a.sum()), linha_identica=int(b.sum()), duplicado_logico=int(c.sum()))
    return out, removidos


def tipar(df: pd.DataFrame) -> pd.DataFrame:
    """Converte cada coluna para o tipo certo (estava tudo como texto)."""
    df = df.copy()
    # A fonte traz ids redondos em notação científica ("6e+05" = 600000): converte via float e
    # confere que não houve perda (todo id tem que ser inteiro e continuar único).
    id_num = pd.to_numeric(df["id"], errors="raise")
    notacao_cientifica = int(df["id"].str.contains("e", case=False).sum())
    assert (id_num % 1 == 0).all()
    df["id"] = id_num.astype("int64")
    assert df["id"].is_unique
    df["data"] = pd.to_datetime(df["data"], format="%Y-%m-%d")
    df["data_hora"] = pd.to_datetime(df["data"].dt.strftime("%Y-%m-%d") + " " + df["horario"], format="%Y-%m-%d %H:%M:%S")
    df["horario"] = df["data_hora"].dt.time
    # vírgula decimal -> ponto
    for c in ["km", "latitude", "longitude"]:
        df[c] = pd.to_numeric(df[c].str.replace(",", ".", regex=False), errors="raise").astype("Float64")
    df["br"] = pd.to_numeric(df["br"]).astype("Int16")
    for c in COLS_CONTAGEM:
        df[c] = pd.to_numeric(df[c], errors="raise").astype("int16")
    df["area_urbana"] = df["area_urbana"].map({"Sim": True, "Não": False}).astype("boolean")  # Sim = área urbana
    df["dia_semana"] = pd.Categorical(df["dia_semana"], categories=ORDEM_DIAS, ordered=True)
    df["fase_dia"] = pd.Categorical(df["fase_dia"], categories=ORDEM_FASE, ordered=True)
    df["classificacao_acidente"] = pd.Categorical(df["classificacao_acidente"], categories=ORDEM_CLASSIF, ordered=True)
    for c in ["municipio", "causa_acidente", "tipo_acidente", "sentido_via", "condicao_meteorologica",
              "tipo_pista", "tracado_via", "regional", "delegacia", "uop"]:
        df[c] = df[c].astype("category")
    df["ano_arquivo"] = df["ano_arquivo"].astype("int16")
    registrar("6. Tipagem", len(df), len(df), ids_notacao_cientifica=notacao_cientifica, tipos={c: str(t) for c, t in df.dtypes.items()})
    return df


def _tracado_canonico(s) -> str:
    if pd.isna(s):
        return s
    return ";".join(sorted({p.strip() for p in str(s).split(";") if p.strip()}))


def normalizar(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalização estrutural:
    - rótulos de causa harmonizados entre os anos;
    - tracado_via (multivalorado, sem ordem) -> forma canônica ordenada + colunas booleanas
      + tabela-ponte acidente_tracado (1 linha por acidente x elemento do traçado, 1FN)."""
    df = df.copy()

    antes_causas = df["causa_acidente"].nunique()
    df["causa_acidente"] = df["causa_acidente"].astype(str).replace(MAPA_CAUSAS).astype("category")

    antes_trac = df["tracado_via"].nunique()
    df["tracado_via"] = df["tracado_via"].astype(str).map(_tracado_canonico).astype("category")
    ponte = (df[["id", "tracado_via"]].assign(elemento=df["tracado_via"].astype(str).str.split(";"))
             .explode("elemento")[["id", "elemento"]].reset_index(drop=True))
    ponte["elemento"] = ponte["elemento"].astype("category")
    for el in sorted(ponte["elemento"].unique()):
        col = "tracado_" + unicodedata.normalize("NFKD", el).encode("ascii", "ignore").decode().lower().replace(" ", "_")
        df[col] = df["id"].isin(ponte.loc[ponte["elemento"] == el, "id"])

    registrar("7. Normalização estrutural", len(df), len(df),
              causas_distintas=f"{antes_causas} -> {df['causa_acidente'].nunique()}",
              tracado_combinacoes=f"{antes_trac} -> {df['tracado_via'].nunique()}",
              linhas_tabela_ponte=len(ponte))
    return df, ponte


def derivar(df: pd.DataFrame) -> pd.DataFrame:
    """Variáveis que as hipóteses da Etapa 1 vão usar."""
    df = df.copy()
    df["ano"] = df["data"].dt.year.astype("int16")
    df["mes"] = df["data"].dt.month.astype("int8")
    df["hora"] = df["data_hora"].dt.hour.astype("int8")
    df["fim_de_semana"] = df["dia_semana"].isin(["sábado", "domingo"])            # H2
    df["noite"] = df["fase_dia"].isin(["Plena Noite", "Anoitecer"])                # H2
    df["fatal"] = df["mortos"] > 0                                                 # H1–H4
    df["grave"] = (df["mortos"] > 0) | (df["feridos_graves"] > 0)                  # H1–H4
    df["total_envolvidos"] = (df[["mortos", "feridos_leves", "feridos_graves", "ilesos", "ignorados"]]
                              .sum(axis=1).astype("int16"))
    # H1: trecho = BR + faixa de 10 km
    ini = (df["km"] // 10 * 10).astype("Int16")
    df["trecho_10km"] = np.where(df["localizacao_valida"],
                                 "BR-" + df["br"].astype(str) + " km " + ini.astype(str) + "–" + (ini + 10).astype(str),
                                 None)
    df["trecho_10km"] = df["trecho_10km"].astype("category")
    # H4: a base não tem mais "condição da pista"; usamos clima de chuva + causas ligadas a pista molhada.
    df["chuva"] = df["condicao_meteorologica"].isin(CHUVA)
    df["pista_molhada_proxy"] = df["chuva"] | df["causa_acidente"].isin(CAUSAS_PISTA_MOLHADA)
    registrar("8. Variáveis derivadas", len(df), len(df),
              novas=["ano", "mes", "hora", "fim_de_semana", "noite", "fatal", "grave", "total_envolvidos",
                     "trecho_10km", "chuva", "pista_molhada_proxy"])
    return df


def validar(df: pd.DataFrame) -> dict:
    """Regras que a base tratada TEM que cumprir. Levanta AssertionError se alguma falhar."""
    obrig = ["id", "data", "data_hora", "municipio", "causa_acidente", "tipo_acidente",
             "classificacao_acidente", "fase_dia", "condicao_meteorologica", "tipo_pista",
             "tracado_via", "latitude", "longitude", "regional", "delegacia", "uop"] + COLS_CONTAGEM
    checks = {
        "id único": df["id"].is_unique,
        "anos só 2021–2025": df["ano"].between(min(ANOS), max(ANOS)).all(),
        "sem nulo nas colunas obrigatórias": not df[obrig].isna().any().any(),
        "coordenadas dentro do PR": (df["latitude"].between(*BBOX_PR["lat"]) & df["longitude"].between(*BBOX_PR["lon"])).all(),
        "feridos = leves + graves": (df["feridos"] == df["feridos_leves"] + df["feridos_graves"]).all(),
        "fatal <=> classificação 'Com Vítimas Fatais'": (df["fatal"] == (df["classificacao_acidente"] == "Com Vítimas Fatais")).all(),
        "pessoas <= total_envolvidos (diferença = ignorados)": (df["pessoas"] <= df["total_envolvidos"]).all(),
        "dia_semana coerente com a data": (df["data"].dt.dayofweek.map(dict(enumerate(ORDEM_DIAS))) == df["dia_semana"].astype(str)).all(),
        "br/km nulos só onde localizacao_valida=False": (df["localizacao_valida"] == (df["br"].notna() & df["km"].notna())).all(),
    }
    falhas = [k for k, v in checks.items() if not v]
    registrar("9. Validação", len(df), len(df), regras_ok=len(checks) - len(falhas), falhas=falhas)
    assert not falhas, f"Validação falhou: {falhas}"
    return checks


# --------------------------------------------------------------------------- CARGA
ORDEM_FINAL = [
    "id", "data", "horario", "data_hora", "ano", "mes", "hora", "dia_semana", "fim_de_semana",
    "fase_dia", "noite", "br", "km", "trecho_10km", "localizacao_valida", "municipio", "latitude", "longitude",
    "sentido_via", "area_urbana", "tipo_pista", "tracado_via", "causa_acidente", "tipo_acidente",
    "classificacao_acidente", "fatal", "grave", "condicao_meteorologica", "chuva", "pista_molhada_proxy",
    "pessoas", "total_envolvidos", "mortos", "feridos_leves", "feridos_graves", "feridos", "ilesos",
    "ignorados", "veiculos", "regional", "delegacia", "uop", "ano_arquivo",
]


def carregar(df: pd.DataFrame, ponte: pd.DataFrame, removidos: pd.DataFrame, destino: Path = PROCESSED):
    destino.mkdir(parents=True, exist_ok=True)
    cols = ORDEM_FINAL + sorted(c for c in df.columns if c.startswith("tracado_") and c != "tracado_via")
    df = df[cols]
    df.to_parquet(destino / "acidentes_pr_2021_2025.parquet", index=False)      # preserva os tipos
    df.to_csv(destino / "acidentes_pr_2021_2025.csv", index=False, encoding="utf-8")  # p/ Power BI / Excel
    ponte.to_csv(destino / "acidente_tracado.csv", index=False, encoding="utf-8")
    removidos.to_csv(destino / "auditoria_duplicados_removidos.csv", index=False, encoding="utf-8")
    registrar("10. Carga", len(df), len(df), arquivos=sorted(p.name for p in destino.iterdir()))
    (destino / "log_etl.json").write_text(json.dumps(LOG, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return df


def executar() -> pd.DataFrame:
    LOG.clear()
    baixar()
    df = extrair()
    df = padronizar_texto(df)
    df = filtrar(df)
    df = tratar_nulos(df)
    df, removidos = remover_duplicados(df)
    df = tipar(df)
    df, ponte = normalizar(df)
    df = derivar(df)
    validar(df)
    return carregar(df, ponte, removidos)


if __name__ == "__main__":
    final = executar()
    for e in LOG:
        print(f"{e['etapa']:<45} {e['linhas_antes']:>8} -> {e['linhas_depois']:>8}")
    print(f"\nBase final: {final.shape[0]} acidentes x {final.shape[1]} colunas em {PROCESSED}")
