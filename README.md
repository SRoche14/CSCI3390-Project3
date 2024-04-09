# Large Scale Data Processing: Project 3
Authors:
- Ilan Valencius (valencig)
- Steven Roche (sroche14)
- Jason Adhinarta (jasonkena)

## Getting started
Head to [Project 1](https://github.com/CSCI3390Spring2024/project_1) if you're looking for information on Git, template repositories, or setting up your local/remote environments.

- Steven Roche (sroche14)
- Ilan Valencius (valencig)
- Jason Adhinarta (jasonkena)

## 1 - verifyMIS
```
// Linux
spark-submit --class project_3.main --master local[*] target/scala-2.12/project_3_2.12-1.0.jar verify [path_to_graph] [path_to_MIS]

// Unix
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar verify [path_to_graph] [path_to_MIS]
```
Apply `verifyMIS` locally with the parameter combinations listed in the table below and **fill in all blanks**.
|        Graph file       |           MIS file           | Is an MIS? |
| ----------------------- | ---------------------------- | :----------: |
| small_edges.csv         | small_edges_MIS.csv          | :white_check_mark:      |
| small_edges.csv         | small_edges_non_MIS.csv      | :x:        |
| line_100_edges.csv      | line_100_MIS_test_1.csv      | :white_check_mark:        |
| line_100_edges.csv      | line_100_MIS_test_2.csv      | :x:         |
| twitter_10000_edges.csv | twitter_10000_MIS_test_1.csv | :x:         |
| twitter_10000_edges.csv | twitter_10000_MIS_test_2.csv | :white_check_mark:       |

## 2 - LubyMIS
Implement the `LubyMIS` function. The function accepts a Graph[Int, Int] object as its input. You can ignore the two integers associated with the vertex RDD and the edge RDD as they are dummy fields. `LubyMIS` should return a Graph[Int, Int] object such that the integer in a vertex's data field denotes whether or not the vertex is in the MIS, with 1 signifying membership and -1 signifying non-membership. The output will be written as a CSV file to the output path you provide. To execute the function, run the following:
```
// Linux
spark-submit --class project_3.main --master local[*] target/scala-2.12/project_3_2.12-1.0.jar compute [path_to_input_graph] [path_for_output_graph]

// Unix
spark-submit --class "project_3.main" --master "local[*]" target/scala-2.12/project_3_2.12-1.0.jar compute [path_to_input_graph] [path_for_output_graph]
```
Apply `LubyMIS` locally on the graph files listed below and report the number of iterations and running time that the MIS algorithm consumes for **each file**. You may need to include additional print statements in `LubyMIS` in order to acquire this information. Finally, verify your outputs with `verifyMIS`.
|        Graph file       | Running time [s] | Iterations | verifyMIS |
| :-----------------------: | :-: | :-: | :-: |
| small_edges.csv         | 0 | 1 | :white_check_mark: |
| line_100_edges.csv      | 1 | 2 | :white_check_mark: |
| twitter_100_edges.csv   | 1 | 2 | :white_check_mark: | 
| twitter_1000_edges.csv  | 1 | 3 | :white_check_mark: |
| twitter_10000_edges.csv | 1 | 3 | :white_check_mark: |

## 3 - LubyMIS (GCP)
a. Run `LubyMIS` on `twitter_original_edges.csv` in GCP with 3x4 cores. Report the number of iterations, running time, and remaining active vertices (i.e. vertices whose status has yet to be determined) at the end of **each iteration**. You may need to include additional print statements in `LubyMIS` in order to acquire this information. Finally, verify your outputs with `verifyMIS`.  
b. Run `LubyMIS` on `twitter_original_edges.csv` with 4x2 cores and then 2x2 cores. Compare the running times between the 3 jobs with varying core specifications that you submitted in **3a** and **3b**.

