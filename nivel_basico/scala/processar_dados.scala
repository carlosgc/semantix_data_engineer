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
        col("obitosAcumulado") / col("casosAcumulado")
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

// Salvando a primeira visualização
visualizacao1.write.mode(SaveMode.Overwrite).format("hive").saveAsTable("visualizacao1");

// Salvando a segunda visualização
visualizacao2.write.mode(SaveMode.Overwrite).save("/user/covidbr/visualizacao2");

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

tabela_covid.
    where("regiao = 'Brasil'").
    select("regiao", "populacaoTCU2019", "data", "casosAcumulado", "obitosAcumulado", "obitosNovos").
    orderBy(col("data").desc).
    withColumn(
        "Letalidade", 
        col("obitosAcumulado") / col("casosAcumulado")
    ).
    limit(2).
    withColumn(
        "Mortalidade", 
        (col("obitosAcumulado") / col("populacaoTCU2019")) * 100000
    ).
    select(
        col("obitosAcumulado").as("obitos_acumulados"),
        col("obitosNovos").as("casos_novos"),
        col("letalidade").as("letalidade"),
        col("mortalidade").as("mortalidade"),
        from_unixtime(unix_timestamp(col("data"), "yyyy-MM-dd"), "yyyy-MM-dd'T'HH:mm:ss").as("data")
    ).
    write.mode(SaveMode.Overwrite).json("/user/covidbr/visualizacao3_elastic");