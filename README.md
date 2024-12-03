# Million Song Dataset Similarity Analysis

This Spark application computes song similarities based on user listening patterns from the Echo Nest Taste Profile subset of the Million Song Dataset. It implements a scalable approach to finding similar songs using collaborative filtering techniques.

## Overview

The application calculates song similarities by:
1. Analyzing user listening patterns
2. Adjusting for individual user biases by normalizing ratings
3. Computing song-to-song similarities using adjusted cosine similarity
4. Finding the top-K most similar songs for each track

## Features

- Efficient parallel processing using Apache Spark
- Custom partitioning strategy for optimal performance
- Bias-adjusted similarity calculations
- Configurable minimum co-rating threshold
- Top-K similar songs output for each track

## Requirements

- Apache Spark
- Scala
- SBT or similar build tool
- Echo Nest Taste Profile Dataset

## Usage

```
	make switch-standalone
	make local
```

### Parameters

- `user_song_matrix`: Input file containing user-song play counts (format: `userId,songId,playCount`)
- `output_dir`: Directory where results will be saved
- `topK`: Number of similar songs to return for each track

### Input Format

The input file should be a CSV with the following format:
```
userId,songId,playCount
```

### Output Format

The output will be saved as text files with the following format:
```
songId: (similarSongId1,similarity1), (similarSongId2,similarity2), ...
```

## Algorithm Details

1. **Data Loading**: Loads user-song play count matrix
2. **Rating Normalization**: Adjusts play counts by subtracting user means to account for individual listening patterns
3. **Partitioning**: Implements both horizontal and vertical partitioning for efficient computation
4. **Similarity Calculation**: Uses adjusted cosine similarity with the following formula:
   ```
   similarity = dotProduct / (sqrt(norm1) * sqrt(norm2))
   ```
5. **Filtering**: Applies minimum co-rating threshold to ensure reliable similarities
6. **Top-K Selection**: Selects the K most similar songs for each track

## Performance Considerations

- Uses custom partitioning to optimize data distribution
- Implements caching strategies for frequently accessed RDDs
- Filters out insufficient co-ratings (minimum 5 by default)
- Configurable number of partitions (default: 10000)
