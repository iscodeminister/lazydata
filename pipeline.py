
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import HashingTF, Tokenizer, StopWordsRemover

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import regexp_replace, col
sc = SparkContext('local')
spark = SparkSession(sc)

# Prepare training documents from a list of (id, text, label) tuples.
training = spark.read.csv("hdfs://localhost:9000/data/train.csv", header=True).select("text", col("target").cast("double"))
training = training.withColumn("text", regexp_replace("text", r'(\d+)', '--'))
training = training.na.drop()
training = training.filter(training.target.isNotNull())

# Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
tokenizer = Tokenizer(inputCol="text", outputCol="words")
remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
hashingTF = HashingTF(inputCol=remover.getOutputCol(), outputCol="features")
lr = RandomForestClassifier(numTrees=3, maxDepth=2, labelCol="target", featuresCol="features")
pipeline = Pipeline(stages=[tokenizer, remover, hashingTF, lr])

# Fit the pipeline to training documents.
model = pipeline.fit(training)

# Prepare test documents, which are unlabeled (id, text) tuples.
test = spark.read.csv("hdfs://localhost:9000/data/test.csv", header=True).select("id", "text")
test = test.withColumn("text", regexp_replace("text", r'(\d+)', '--'))
test = test.na.drop()

# Make predictions on test documents and print columns of interest.
prediction = model.transform(test)
selected = prediction.select("id", "text", "probability", "prediction")
for row in selected.collect():
    rid, text, prob, prediction = row  # type: ignore
    print(
        "(%d, %s) --> prob=%s, prediction=%f" % (
            int(rid), text, str(prob), prediction   # type: ignore
        )
    )
