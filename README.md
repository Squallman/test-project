# Filter Parquet records

### How to build
```bash
sbt clean assebly
```
The output jar will be in the `target` folder.

### Arguments
| Argument        |             Description              |
| ------------- |:------------------------------------:|
| inputPath      |     Path with input parket files     |
| inputPoiFile      |  Input JSON file with POI locations  |
| daysToFilter | Number of days in the past to filter |
| outputFile |     Output Parquet file location     |

The command line arguments should be in format "name=value" "name2=value2", i.e. outputFile=/Users/cahillt/Desktop/myParquet.parquet numLocations=10000 percentLocationsNear=.5

#### Sample Run
```bash
java -jar target/scala-2.13/test-filter-fatjar-1.0.jar inputPath=/path/to/parket inputPoiFile=/path/to/poi.json daysToFilter=30 outputPath=/path/to/output/parket/files
```