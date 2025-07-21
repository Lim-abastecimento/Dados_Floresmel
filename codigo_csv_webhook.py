from flask import Flask, request, jsonify
from google.cloud import bigquery, storage
import csv
import io
import datetime
import os

app = Flask(__name__)

@app.route("/", methods=["POST"])
def gerar_csv_bigquery():
    try:
        # 1. Consulta ao BigQuery
        client = bigquery.Client()
        query = """
            SELECT produto, loja, estoque, ddv, dias, status
            FROM `abastecimento-465513.dados2025.estoque_unificado`
        """
        query_job = client.query(query)
        results = query_job.result()

        # 2. Prepara CSV em memória
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Produto", "Loja", "Estoque", "DDV", "Dias", "Status"])  # cabeçalho

        for row in results:
            writer.writerow([
                row.produto,
                row.loja,
                row.estoque,
                row.ddv,
                row.dias,
                row.status
            ])

        # 3. Upload para Cloud Storage
        bucket_name = os.environ.get("BUCKET_NAME")
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        filename = f"estoque_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        blob = bucket.blob(filename)
        blob.upload_from_string(output.getvalue(), content_type="text/csv")
        blob.make_public()

        csv_url = blob.public_url

        # 4. Retorno para Vertex AI
        return jsonify({
            "fulfillmentResponse": {
                "messages": [
                    {
                        "text": {
                            "text": [
                                f"📦 Relatório gerado com sucesso!\n\n👉 [Clique aqui para baixar o CSV]({csv_url})"
                            ]
                        }
                    }
                ]
            }
        })

    except Exception as e:
        return jsonify({
            "fulfillmentResponse": {
                "messages": [
                    {
                        "text": {
                            "text": [f"❌ Erro ao gerar o relatório: {str(e)}"]
                        }
                    }
                ]
            }
        }), 500

#📦 requirements.txt
#txt
#Copiar
#Editar
#flask==2.3.3
#google-cloud-bigquery==3.17.2
#google-cloud-storage==2.16.0
#🌐 Variáveis de ambiente esperadas
#Variável	Descrição
#BUCKET_NAME	Nome do bucket onde o CSV será salvo

#🚀 Como implantar no Cloud Functions (2ª geração)
#bash
#Copiar
#Editar
#gcloud functions deploy gerarCSVEstoque \
#--gen2 \
#--runtime=python310 \
#--region=us-central1 \
# --entry-point=app \
#--source=. \
#--trigger-http \
#--allow-unauthenticated \
#--set-env-vars=BUCKET_NAME=nome-do-seu-bucket
#Certifique-se de que a função tem permissão para acessar o BigQuery e o Cloud Storage. Use a role BigQuery Data Viewer e Storage Object Admin na conta de serviço da função.

#🧠 Quer filtrar por loja ou produto?
#Se quiser que o webhook gere CSV só quando tiver mais de uma loja, ou com filtros dinâmicos, posso adaptar a consulta pra isso:

#sql
#Copiar
#Editar
#SELECT produto, loja, estoque, ddv, dias, status
#FROM `abastecimento-465513.dados2025.estoque_unificado`
#WHERE loja IN ('Loja Centro', 'Loja Estação')
#É só me dizer como você quer parametrizar os filtros (pelo usuário, pelo intent, etc).



