# Projeto Final de Spark - Nível Básico

O projeto foi implementado completando os passos descritos no [documento](../docs/projeto_final_spark.pdf) que descrição.

As tarefas de processamento e preparação dos dados foram feitos na linguagem Scala. O código final pode ser encontrado em [scala/processar_dados.scala](scala/processar_dados.scala).

As tarefas de visualização dos dados foram implementadas em um notebook, utilizando a interface PySpark. O notebook pode ser encontrado em [pyspark/visualizacoes.ipynb](pyspark/visualizacoes.ipynb).

Os passos realizados para a implementação deste projeto estão descritos abaixo.

### 1. Enviar os dados para o hdfs

```bash
# Criação de diretório dentro do hdfs para armazenamento dos dados
$ docker exec -it jupyter-spark hdfs dfs -mkdir /user/covidbr

# Cópia dos dados para o container
$ docker cp dados/HIST_PAINEL_COVIDBR_06jul2021 namenode:/input

# Envio dos dados para o hdfs
$ docker exec -it namenode hdfs dfs -put /input/HIST_PAINEL_COVIDBR_06jul2021 /user/covidbr
```

### 2. Otimizar todos os dados do hdfs para uma tabela Hive particionada por município.

```scala
// Importação de pacotes
import org.apache.spark.sql.types._;
import org.apache.spark.sql.SaveMode;

// Alterando a configuração para habilitar a partição dinâmica de tabelas Hive
spark.sqlContext.setConf("hive.exec.dynamic.partition", "true");
spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict");

// Criação de esquema dos dados que serão carregados
val data_schema = new StructType().
    add("regiao", StringType).
    add("estado", StringType).
    add("municipio", StringType).
    add("coduf", IntegerType).
    add("codmun", IntegerType).
    add("codRegiaoSaude", IntegerType).
    add("nomeRegiaoSaude", StringType).
    add("data", StringType).
    add("semanaEpi", IntegerType).
    add("populacaoTCU2019", IntegerType).
    add("casosAcumulado", IntegerType).
    add("casosNovos", IntegerType).
    add("obitosAcumulado", IntegerType).
    add("obitosNovos", IntegerType).
    add("Recuperadosnovos", IntegerType).
    add("emAcompanhamentoNovos", IntegerType).
    add("interior/metropolitana", IntegerType);

// Carregamento dos dados
val covid_br = spark.read.
    option("delimiter", ";").
    option("header", "true").
    schema(data_schema).
    csv("/user/covidbr/HIST_PAINEL_COVIDBR_06jul2021");

// Seleção das colunas que serão salvas na tabela
val covid_br_otimizado = covid_br.select("regiao", "estado", "municipio", "populacaoTCU2019", "data", "casosAcumulado", "casosNovos", "obitosAcumulado", "obitosNovos", "Recuperadosnovos", "emAcompanhamentoNovos");

// Criação de tabela particionada por estados e municípios
covid_br_otimizado.write.mode(SaveMode.Overwrite).partitionBy("estado").bucketBy(8, "municipio").sortBy("municipio").saveAsTable("covid_br");
```

### 3. Criar as 3 vizualizações pelo Spark com os dados enviados para o HDFS

```scala
// Carregamento da tabela salva no hdfs
val tabela_covid = spark.table("covid_br");

// Criação da primeira visualização
val visualizacao1 = tabela_covid.
    where("regiao = 'Brasil'").
    select("regiao", "data", "Recuperadosnovos", "emAcompanhamentoNovos").
    orderBy(col("data").desc).
    select(
        col("Recuperadosnovos").as("casos_recuperados"),
        col("emAcompanhamentoNovos").as("em_acompanhamento")
    ).limit(1);

// Criação da segunda visualização
val visualizacao2 = tabela_covid.
    where("regiao = 'Brasil'").
    select("regiao", "populacaoTCU2019", "data", "casosAcumulado", "casosNovos").
    orderBy(col("data").desc).
    limit(1).
    withColumn(
        "Incidencia", 
        (col("casosAcumulado") / col("populacaoTCU2019")) * 100000
    ).
    select(
        col("casosAcumulado").as("acumulado"),
        col("casosNovos").as("casos_novos"),
        col("Incidencia").as("incidencia")
    );

// Criação da terceira visualização
val visualizacao3 = tabela_covid.
    where("regiao = 'Brasil'").
    select("regiao", "populacaoTCU2019", "data", "casosAcumulado", "obitosAcumulado", "obitosNovos").
    orderBy(col("data").desc).
    limit(1).
    withColumn(
        "Letalidade", 
        (col("obitosAcumulado") / col("casosAcumulado")) * 100
    ).
    withColumn(
        "Mortalidade", 
        (col("obitosAcumulado") / col("populacaoTCU2019")) * 100000
    ).
    select(
        col("obitosAcumulado").as("obitos_acumulados"),
        col("obitosNovos").as("casos_novos"),
        col("letalidade").as("letalidade"),
        col("mortalidade").as("mortalidade")
    );
```

### 4. Salvar a primeira visualização como tabela Hive

```scala
// Salvando a primeira visualização
visualizacao1.write.mode(SaveMode.Overwrite).format("hive").saveAsTable("visualizacao1");
```

### 5. Salvar a segunda visualização com formato parquet e compressão snappy

```scala
// Salvando a segunda visualização
visualizacao2.write.mode(SaveMode.Overwrite).save("/user/covidbr/visualizacao2");
```

### 6. Salvar a terceira visualização em um tópico no Kafka

```scala
// Salvando a terceira visualização
visualizacao3.select(
    format_string(
        "{obitos_acumulados:%s, casos_novos:%s, letalidade:%s, mortalidade:%s}", 
        col("obitos_acumulados"), 
        col("casos_novos"), 
        col("letalidade"), 
        col("mortalidade")
    ).as("value")
).
write.format("kafka").
option("kafka.bootstrap.servers", "kafka:9092").
option("topic", "covid-obitos-confirmados").
save();

// Salvando a terceira visualização no hdfs
visualizacao3.write.mode(SaveMode.Overwrite).save("/user/covidbr/visualizacao3");
```

### 7. Criar a visualização pelo Spark com os dados enviados para o HDFS:

```scala
// Criação da quarta visualização
val visualizacao4 = tabela_covid.
    select("regiao", "populacaoTCU2019", "data", "casosAcumulado", "obitosAcumulado").
    where("data = (select max(data) from covid_br)").
    groupBy("regiao", "data").
    agg(
        sum("populacaoTCU2019").alias("populacaoTCU2019"),
        sum("casosAcumulado").alias("casosAcumulado"),
        sum("obitosAcumulado").alias("obitosAcumulado")
    ).
    withColumn(
        "incidencia", 
        (col("casosAcumulado") / col("populacaoTCU2019")) * 100000
    ).
    withColumn(
        "mortalidade", 
        (col("obitosAcumulado") / col("populacaoTCU2019")) * 100000
    ).
    select(
        col("regiao"),
        col("casosAcumulado").as("casos"),
        col("obitosAcumulado").as("obitos"),
        col("incidencia").as("incidencia"),
        col("mortalidade").as("mortalidade"),
        from_unixtime(unix_timestamp(col("data"), "yyyy-MM-dd"), "dd/MM/yyyy HH:mm").as("atualizacao")
    );

// Salvando a quarta visualização
visualizacao4.write.mode(SaveMode.Overwrite).save("/user/covidbr/visualizacao4");
```

### 8. Salvar a visualização do exercício 6 em um tópico no Elastic

> Essa tarefa não está completa. Ela foi realizada copiando os dados da visualização três e inserindo o comando abaixo na ferramenta Dev Tools do Elastic. Entretando, ela seria melhor implementada utilizando uma integração entre o Spark e o Elastic. 

```
PUT covid_obitos_confirmados/_create/1
{
  "obitos_acumulados": 526892,
  "casos_novos": 1780,
  "letalidade": 2.794439569525667,
  "mortalidade": 250.72529543290204
}
```

### 9. Criar um dashboard no Elastic para visualização dos novos dados enviados

> Essa tarefa está pendente. O tópico do Elastic onde foram inseridos os dados não ficou disponível na ferramenta Dashboard do Kibana para a criação do dashboard.