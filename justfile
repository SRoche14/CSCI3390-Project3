default:
    just --list

build:
    sbt clean package

set export
LOCAL_DIRS := "."

small:
    spark-submit --class project_3.main --master local[*] target/scala-2.12/project_3_2.12-1.0.jar compute ./small_edges.csv ./small_edges_output.csv
    # spark-submit --class project_3.main --master local[*] target/scala-2.12/project_3_2.12-1.0.jar verify ./small_edges.csv ./small_edges_output.csv

line:
    spark-submit --class project_3.main --master local[*] target/scala-2.12/project_3_2.12-1.0.jar compute ./line_100_edges.csv ./line_100_edges_output.csv
    # spark-submit --class project_3.main --master local[*] target/scala-2.12/project_3_2.12-1.0.jar verify ./line_100_edges.csv ./line_100_edges_output.csv

twitter_100:
    spark-submit --class project_3.main --master local[*] target/scala-2.12/project_3_2.12-1.0.jar compute ./twitter_100_edges.csv ./twitter_100_edges_output.csv
    # spark-submit --class project_3.main --master local[*] target/scala-2.12/project_3_2.12-1.0.jar verify ./twitter_100_edges.csv ./twitter_100_edges_output.csv

twitter_1000:
    spark-submit --class project_3.main --master local[*] target/scala-2.12/project_3_2.12-1.0.jar compute ./twitter_1000_edges.csv ./twitter_1000_edges_output.csv
    # spark-submit --class project_3.main --master local[*] target/scala-2.12/project_3_2.12-1.0.jar verify ./twitter_1000_edges.csv ./twitter_1000_edges_output.csv

twitter_10000:
    spark-submit --class project_3.main --master local[*] target/scala-2.12/project_3_2.12-1.0.jar compute ./twitter_10000_edges.csv ./twitter_10000_edges_output.csv
    # spark-submit --class project_3.main --master local[*] target/scala-2.12/project_3_2.12-1.0.jar verify ./twitter_10000_edges.csv ./twitter_10000_edges_output.csv


twitter_original:
    spark-submit --driver-memory 10g --class project_3.main --master local[*] target/scala-2.12/project_3_2.12-1.0.jar compute ./twitter_original_edges.csv ./twitter_original_edges_output.csv
    # spark-submit --driver-memory 10g --class project_3.main --master local[*] target/scala-2.12/project_3_2.12-1.0.jar verify ./twitter_original_edges.csv ./twitter_original_edges_output.csv
