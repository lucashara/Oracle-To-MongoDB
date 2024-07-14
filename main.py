import os
import cx_Oracle
import pymongo
import logging
import locale
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime, timedelta
import time
import argparse

# Configurações gerais
locale.setlocale(locale.LC_ALL, "pt_BR.UTF-8")
logging.basicConfig(
    handlers=[
        logging.FileHandler("Oracle-to-MongoDB.log", "a", "utf-8"),
        logging.StreamHandler(),
    ],
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
)

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()


def formata_tempo_completo(segundos):
    """Converte segundos para o formato 'dias:horas:minutos:segundos'."""
    dias, resto = divmod(segundos, 86400)
    horas, resto = divmod(resto, 3600)
    minutos, segundos = divmod(resto, 60)
    return f"{int(dias)}d {int(horas)}h {int(minutos)}min {int(segundos)}s"


def converte_lob_para_string_bytes(dados):
    """Converte dados LOB do Oracle em strings ou bytes para inserção no MongoDB."""
    dados_convertidos = []
    for linha in dados:
        nova_linha = []
        for valor in linha:
            if isinstance(valor, cx_Oracle.LOB):
                nova_linha.append(valor.read())
            elif isinstance(valor, datetime):
                nova_linha.append(valor.strftime("%d-%m-%Y %H:%M:%S"))
            else:
                nova_linha.append(valor)
        dados_convertidos.append(tuple(nova_linha))
    return dados_convertidos


def executa_consulta_leitura(conexao, consulta):
    """Executa uma consulta SQL e retorna os dados."""
    cursor = conexao.cursor()
    try:
        cursor.execute(consulta)
        return cursor.fetchall()
    except Exception as e:
        logging.error(f"Ocorreu um erro '{e}' ao executar a consulta: {consulta}")
        return None
    finally:
        cursor.close()


def processa_arquivos_sql(diretorio_sql="sql/"):
    inicio_total = time.time()
    logging.info("Iniciando processamento dos arquivos SQL.")

    # Inicializa as conexões
    try:
        dsn_oracle = cx_Oracle.makedsn(
            os.getenv("DB_HOSTNAME"),
            os.getenv("DB_PORT"),
            service_name=os.getenv("DB_SERVICE_NAME"),
        )
        conexao_oracle = cx_Oracle.connect(
            user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"), dsn=dsn_oracle
        )
        logging.info("Conexão com Oracle estabelecida com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao conectar ao Oracle: {e}")
        return

    try:
        mongo_client = pymongo.MongoClient(
            host=os.getenv("MONGO_HOST"),
            port=int(os.getenv("MONGO_PORT")),
            username=os.getenv("MONGO_USER"),
            password=os.getenv("MONGO_PASSWORD"),
        )
        mongo_db = mongo_client[os.getenv("MONGO_DB")]
        logging.info("Conexão com MongoDB estabelecida com sucesso.")
    except Exception as e:
        logging.error(f"Erro ao conectar ao MongoDB: {e}")
        return

    arquivos_sql = list(Path(diretorio_sql).glob("*.sql"))

    for arquivo_sql in arquivos_sql:
        inicio_arquivo = time.time()
        nome_arquivo = arquivo_sql.stem

        try:
            with open(arquivo_sql, "r", encoding="utf-8") as arquivo:
                consulta_sql = arquivo.read()
            logging.info(f"Arquivo {nome_arquivo} lido com sucesso.")
        except Exception as e:
            logging.error(f"Erro ao ler o arquivo {nome_arquivo}: {e}")
            continue

        dados_oracle = executa_consulta_leitura(conexao_oracle, consulta_sql)
        if dados_oracle is None or len(dados_oracle) == 0:
            logging.error(
                f"Nenhum dado foi obtido do Oracle para o arquivo {nome_arquivo}. Processamento desse arquivo ignorado."
            )
            continue

        dados_oracle_convertidos = converte_lob_para_string_bytes(dados_oracle)
        cursor_oracle = conexao_oracle.cursor()
        cursor_oracle.execute(consulta_sql)
        nomes_colunas = [coluna[0] for coluna in cursor_oracle.description]
        cursor_oracle.close()

        documentos_mongo = [
            {nomes_colunas[i]: valor for i, valor in enumerate(linha)}
            for linha in dados_oracle_convertidos
        ]
        collection = mongo_db[nome_arquivo]

        try:
            collection.insert_many(documentos_mongo)
            logging.info(
                f"Dados inseridos com sucesso na coleção {nome_arquivo} do MongoDB."
            )
        except Exception as e:
            logging.error(
                f"Erro ao inserir dados na coleção {nome_arquivo} do MongoDB: {e}"
            )

        fim_arquivo = time.time()

        logging.info(
            f"Arquivo {nome_arquivo}: linhas Oracle={len(dados_oracle)}, documentos inseridos={len(documentos_mongo)}, tempo={formata_tempo_completo(fim_arquivo - inicio_arquivo)}"
        )

    fim_total = time.time()
    logging.info(
        f"Processamento total concluído. Tempo total: {formata_tempo_completo(fim_total - inicio_total)}."
    )

    conexao_oracle.close()
    mongo_client.close()


def modo_manual():
    logging.info("Modo manual iniciado")
    processa_arquivos_sql()
    logging.info("Modo manual finalizado")


def modo_diario(hora):
    now = datetime.now()
    target_time = now.replace(
        hour=int(hora.split(":")[0]),
        minute=int(hora.split(":")[1]),
        second=0,
        microsecond=0,
    )
    if now > target_time:
        target_time += timedelta(days=1)
    delay = (target_time - now).total_seconds()
    logging.info(
        f"Processamento diário agendado para {target_time.strftime('%d/%m/%Y %H:%M:%S')} (em {formata_tempo_completo(delay)})"
    )
    time.sleep(delay)
    while True:
        logging.info("Modo diário iniciado")
        processa_arquivos_sql()
        logging.info("Modo diário finalizado")
        time.sleep(24 * 3600)


def modo_por_intervalo(intervalo):
    horas, minutos = map(int, intervalo.split(":"))
    intervalo_segundos = horas * 3600 + minutos * 60
    logging.info(f"Processamento agendado a cada {intervalo} horas")
    while True:
        logging.info("Modo por intervalo iniciado")
        processa_arquivos_sql()
        logging.info("Modo por intervalo finalizado")
        logging.info(
            f"Próximo processamento em {formata_tempo_completo(intervalo_segundos)}"
        )
        time.sleep(intervalo_segundos)


# Argumentos do script
parser = argparse.ArgumentParser(
    description="Migração de dados do Oracle para o MongoDB"
)
parser.add_argument(
    "--modo",
    required=True,
    choices=["manual", "diario", "por_intervalo"],
    help="Modo de operação do script",
)
parser.add_argument(
    "--tempo",
    help="Hora para o modo diário (HH:MM) ou intervalo para o modo por intervalo (HH:MM)",
)

args = parser.parse_args()

if args.modo == "manual":
    modo_manual()
elif args.modo == "diario" and args.tempo:
    modo_diario(args.tempo)
elif args.modo == "por_intervalo" and args.tempo:
    modo_por_intervalo(args.tempo)
else:
    logging.error(
        "Argumentos inválidos. Certifique-se de fornecer --tempo para modos diario e por_intervalo."
    )
