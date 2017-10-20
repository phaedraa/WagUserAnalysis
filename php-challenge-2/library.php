<?php
const API_SECRET = "21db65a65e204cca7b5afcbad91fea59";
date_default_timezone_set("UTC");

function create_db_table($pdo)
{
    $sql = "DROP TABLE IF EXISTS scores";
    $pdo->exec($sql);

    $sql = "
    CREATE TABLE scores
    (
        `id`          MEDIUMINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
        `user_id`     MEDIUMINT UNSIGNED NOT NULL,
        `score`       TINYINT UNSIGNED NOT NULL,
        `date`        DATE NOT NULL
    )
    ";
    $pdo->exec($sql);
}

function populate_db_from_csv($pdo, $file)
{
    $sql = "DELETE FROM scores";
    $pdo->exec($sql);


    $sql = "
    INSERT INTO scores (`user_id`, `score`, `date`)
    VALUES (:user_id, :score, :date)
    ";

    $handle = fopen($file, "r");
    while (($data = fgetcsv($handle)) !== false) {
        // skip blank lines
        if ($data !== [null]) {
            $pdo->prepare($sql)->execute($data);
        }
    }
    fclose($handle);
}

function make_request($payload, $secret)
{
    $payload   = json_encode($payload);
    $signature = hash_hmac('sha256', $payload, $secret);
    $request   = base64_encode($signature).'.'.base64_encode($payload);

    return strtr($request, '+/', '-_');
}
