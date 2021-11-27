//Load csv file without heading
movies = LOAD '/Movie_data/movie.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') as ( movieId: int, title: chararray, generes: chararray);
dump movies;

ratings = LOAD '/Movie_data/rating.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER') as ( userId: int, movieId: int, rating: float, timestamp: chararray);
dump ratings;

p_rating = FOREACH ratings GENERATE movieId, userId;
g_rating = GROUP p_rating BY movieId;
g_rating_all = GROUP p_rating All;
count_ratings = FOREACH g_rating GENERATE group AS movieId, COUNT(p_rating.userId) AS num_rating;

get_movies = FOREACH movies GENERATE movieId, title;
join_movie_rating = JOIN get_movies BY movieId, count_ratings BY movieId;
sort_ratings = ORDER count_ratings BY num_rating DESC;
dump sort_ratings;
get_movies = FOREACH movies GENERATE movieId, title;
join_movie_rating = JOIN get_movies BY movieId, count_ratings BY movieId;
dump join_movie_rating;
sort_result = ORDER join_movie_rating BY num_rating DESC;
dump sort_result;

STORE sort_result INTO '/Movie_data/popular_movie_user/' Using PigStorage(',');
