import glob
import os
import sys

spark_home = os.environ['SPARK_HOME']
spark_python = os.path.join(spark_home, 'python')
py4j = glob.glob(os.path.join(spark_python, 'lib', 'py4j-*.zip'))[0]
sys.path[:0] = [spark_python, py4j]
