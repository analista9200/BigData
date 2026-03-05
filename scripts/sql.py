from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, sum, round

#===================================================
# 1- Criando SparkSession
#===================================================
spark = SparkSession.builder \
    .appName("BigData_FoodDelivery") \
    .master("local[*]") \
    .getOrCreate()

#===================================================
# 2- Caminho do arquivo
#===================================================

caminho_arquivo = "dados/dados_loja_fooddelivery.csv"

#===================================================
# 3- Leitura do CSV (atenção ao separador ;)
#===================================================

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("sep", ";") \
    .csv(caminho_arquivo)

#===================================================
# 4- Visualizar estrutura dos dados
#===================================================
print("\nPrimeiros registros:")   
df.show(5)

print("\n Estrutura do DataFrame")
df.printSchema()

#===================================================
# 4. Quantidade total de registros
#===================================================

total_registros = df.count()
print(f"\nTotal de registros: {total_registros}")

#===================================================
# Análise via Spark SQL
#===================================================
df.createOrReplaceTempView("pedidos")

resultado_sql = spark.sql("""
    SELECT
        cidade,
        COUNT(*) AS total_pedidos,
        ROUND(AVG(valor_pedido),2) AS ticket_medio,
        ROUND(AVG(avaliacao_cliente),2) AS avaliacao_media
    FROM pedidos
    WHERE status = 'Entregue'
    GROUP BY cidade
    ORDER BY total_pedidos DESC
""")

resultado_sql.show()