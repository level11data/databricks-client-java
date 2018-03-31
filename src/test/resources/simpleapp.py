from pyspark.sql import SparkSession

def get_spark():
    return (SparkSession.builder
                .appName("simpleapp")
                .master("local")
                .getOrCreate())

from pyspark import SparkConf, SparkContext
import sys

def main(sc, args):
    print("SimpleApp Arguments")
    for x in args:
      print x
      
    simple_data = [
      ("Group A", "Section 1", 50),
      ("Group B", "Section 2", 75),
      ("Group A", "Section 1", 25),
      ("Group C", "section 2", 75)
    ]
    
    simple_df = get_spark().createDataFrame(
      simple_data,
      ["Group", "Section", "Amount"]
    )

    simple_df.show() 
      
if __name__ == "__main__":

  # Configure Spark
  sc = get_spark()
  # Execute Main functionality
  main(sc, sys.argv)
  