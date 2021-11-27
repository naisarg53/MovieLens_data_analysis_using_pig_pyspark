//Load csv file without heading
movies = LOAD '/Movie_data/movie.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') as ( movieId: int, title: chararray, generes: chararray);
dump movies;

ratings = LOAD '/Movie_data/rating.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') as ( userId: int, movieId: int, rating: float, timestamp: chararray);
dump ratings;

p_rating = FOREACH ratings GENERATE movieId, userId, rating;
g_rating = GROUP p_rating BY movieId;
g_rating_all = GROUP p_rating All;

count_ratings = FOREACH g_rating_all GENERATE group AS movieId,COUNT(p_rating.userId) AS num_rating, AVG(p_rating.rating) AS avg_rating;

get_movies = FOREACH movies GENERATE movieId, title;
join_movie_rating = JOIN get_movies BY movieId, count_ratings BY movieId;
dump join_movie_rating;

sort_result = ORDER join_movie_rating BY avg_rating DESC, num_rating DESC;

filter_result = FILTER sort_result BY num_rating > 50000;
sort_filter = ORDER filter_result BY avg_rating DESC;

STORE sort_filter INTO '/Movie_data/top_rated_movie/' Using PigStorage(',');
