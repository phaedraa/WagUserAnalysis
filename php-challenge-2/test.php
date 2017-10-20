<?php
include "library.php";
include "solution.php";

assert_options(ASSERT_ACTIVE, true);
assert_options(ASSERT_WARNING, false);
assert_options(ASSERT_BAIL, true);
assert_options(ASSERT_CALLBACK, function ($path, $line, $message) {
    echo "$path:$line   ERROR"."\n";
});


assert(function_exists("parse_request"));
assert(function_exists("dates_with_at_least_n_scores"));
assert(function_exists("users_with_top_score_on_date"));
assert(function_exists("dates_when_user_was_in_top_n"));

// check encode/decode of the same payload
for ($i = 0; $i < 1000; $i++) {
    $payload = ["s" => "string ".$i, "b" => (bool)($i % 2), "i" => $i, "f" => $i / 10];
    $request = make_request($payload, API_SECRET);

    assert(parse_request($request, API_SECRET) === $payload); // original
    assert(parse_request(strrev($request), API_SECRET) === false); // reverse
    assert(parse_request(substr($request, 1, -1), API_SECRET) === false); // shortened
}


$pdo = new PDO("sqlite:challenge.sqlite");
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

create_db_table($pdo);
populate_db_from_csv($pdo, "./data/data-1.csv");

$correct = ["2017-09-02", "2017-09-01", "2017-08-31"];
$actual  = dates_with_at_least_n_scores($pdo, 1);
assert($correct == $actual);

$correct = ["2017-09-02", "2017-09-01"];
$actual  = dates_with_at_least_n_scores($pdo, 2);
assert($correct == $actual);

$correct = ["2017-09-02"];
$actual  = dates_with_at_least_n_scores($pdo, 3);
assert($correct == $actual);


populate_db_from_csv($pdo, "./data/data-2.csv");

$correct = ["1"];
$actual  = users_with_top_score_on_date($pdo, "2017-08-31");
assert($correct == $actual);

$correct = ["2"];
$actual  = users_with_top_score_on_date($pdo, "2017-09-01");
assert($correct == $actual);

$correct = ["1", "2"];
$actual  = users_with_top_score_on_date($pdo, "2017-09-02");
assert($correct == $actual);


populate_db_from_csv($pdo, "./data/data-3.csv");

$correct = ["2017-09-02", "2017-08-31"];
$actual  = dates_when_user_was_in_top_n($pdo, 1, 1);
assert($correct == $actual);

$correct = ["2017-09-02", "2017-09-01"];
$actual  = dates_when_user_was_in_top_n($pdo, 2, 1);
assert($correct == $actual);

$correct = ["2017-09-02", "2017-09-01", "2017-08-31"];
$actual  = dates_when_user_was_in_top_n($pdo, 1, 2);
assert($correct == $actual);

$correct = ["2017-09-02", "2017-09-01"];
$actual  = dates_when_user_was_in_top_n($pdo, 2, 2);
assert($correct == $actual);

$correct = [];
$actual  = dates_when_user_was_in_top_n($pdo, 3, 2);
assert($correct == $actual);

$correct = ["2017-09-02"];
$actual  = dates_when_user_was_in_top_n($pdo, 3, 3);
assert($correct == $actual);



echo "All tests pass, nice job!\n";
unlink("challenge.sqlite");
