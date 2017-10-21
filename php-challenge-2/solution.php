<?php

/**
* This set class is purely for declaring or adding values;
* it does not handle removal.
*/
class set {
    function __construct($list) {
    	$this->set = [];
    	$this->size = 0;
    	foreach ($list as $_ => $val) {
    	    $this->add($val);
    	}
    }

    function add($val) {
    	if (!array_key_exists($val, $this->set)) {
    	    $this->set[$val] = true;
    	    $this->size++;
    	}
    }

    function toList() {
    	$list = [];
    	foreach ($this->set as $key => $_) {
    	    $list[] = $key;
    	}
    	return $list;
    }

    function size() {
    	return $this->size;
    }

    function has($val) {
    	return array_key_exists($val, $this->set);
    }
}


function fetch_payload($request) {
    $splitter = strpos($request, '.');
    if ($splitter != 88) {
    	return false;
    }
    try {
    	$payload = substr($request, $splitter + 1);
    } catch (Exception $e) {
    	return false;
    }

    if (substr($payload, strlen($req) - 1) == '==') {
    	return false;
    }

    return $payload;
}


function decode_payload($payload) {
    $payload = base64_decode($payload);
    return (array)json_decode($payload);
}


function parse_request($request, $secret) {
    $payload = fetch_payload($request);
    $parsed = decode_payload($payload);

    return count($parsed) > 0 ? $parsed : false;
}


function fetch_dates_with_at_least_n_scores_query($num_scores) {
    $query =
        "SELECT count(score) AS top_score, date " .
        "FROM scores GROUP BY date " .
        "HAVING count(score) >= %s " .
        "ORDER BY date DESC";

    return sprintf($query, $num_scores);
}


function fetch_dates_from_results($results) {
    $rows = $results->fetchAll(PDO::FETCH_ASSOC);
    $dates = [];
    foreach ($rows as $_ => $row) {
        $dates[] = $row['date'];
    }
    return $dates;
}


function dates_with_at_least_n_scores($pdo, $num_scores) {
    if ($num_scores < 0) {
    	throw new Exception(
    	   sprintf('Invalid number of scores %s', $num_scores)
    	);
    }
    
    try {
    	$results = $pdo->query(
    	    fetch_dates_with_at_least_n_scores_query($num_scores)
    	);
    } catch (PDOException $e) {
    	printf(
    	    'Failed to query dates_with_at_least_n_scores',
    	    'results due to exception: %s',
    	    $e->getMessage()
    	);
    	return false;
    }

    return fetch_dates_from_results($results);
}



function is_valid_date($date) {
    $date_pieces = explode('-', $date);
    # month, day, year
    return checkdate(
        $date_pieces[1],
        $date_pieces[2],
        $date_pieces[0]
    );
}


function fetch_users_with_top_score_on_date_query($date) {
    $query =
    	"SELECT DISTINCT user_id " .
    	"FROM scores INNER JOIN ( " .
    	    "SELECT MAX(score) AS top_score " .
    	    "FROM scores " .
    	    "WHERE date = '%s' " .
    	") max_score " .
    	"ON score = max_score.top_score " .
    	"AND date = '%s'";

    return sprintf($query, $date, $date);
}    


function extract_user_ids_from_results($results) {
    $rows = $results->fetchAll(PDO::FETCH_ASSOC);
    $users = [];
    foreach ($rows as $_ => $row) {
    	$users[] = $row['user_id'];
    }
    return $users;
}


function users_with_top_score_on_date($pdo, $date) {
    if (!is_valid_date($date)) {
    	printf('Invalid date: %s', (string)$date);
    	return false;
    }

    try {
    	$results = $pdo->query(
    	    fetch_users_with_top_score_on_date_query($date)
    	);
    } catch (PDOException $e) {
    	printf(
    	    'Failed to query users_with_top_score_on_date',
    	    'results due to exception: %s',
    	    $e->getMessage()
    	);
    	return false;
    }

    return extract_user_ids_from_results($results);
}


function fetch_data_ordered_by_date_score_query($user_id, $rank) {
    // Note: This returns rows w/ desired results, but syntactically
    // appears to be incongruent with PDO/sqlite, hence the reason
    // for the super inefficent and high data storage method.
    //$query_full =
    //	"SELECT date " .
    //	"FROM ( " .
    //		"SELECT " .
    //	        "user_id, " .
    //	        "date, " .
    //	        "score, " .
    //	        "@rank := IF(@prev_rank = date, @rank + 1, 1) AS rank, " .
    //	        "@prev_rank := date " .
    //		"FROM scores " .
    //		"JOIN (SELECT @prev_rank := NULL, @rank := 0) AS rank_init " .
    //		"ORDER BY date, score DESC, user_id " .
    //	") AS top " .
    //	"WHERE rank <= %s " .
    //	"AND user_id = %s";
    $query =
    	"SELECT " .
       	    "user_id, " .
            "date, " .
            "score " .
    	"FROM scores " .
        "ORDER BY date DESC, score DESC, user_id";

    return sprintf($query);
}


function _parse_dates_for_user_in_top_n_alt($results, $user_id, $rank) {
    // NOT used. Seen notes in parse_dates_for_user_in_top_n on why.
    $users_dates = new set([]);
    $update_users_dates = function($rid, $uid, $date, $list) {
    	if ($rid == $uid) {
    	    $list->add($date);
    	}
    };
    
    $date_to_ranks = [];
    $rows = $results->fetchAll(PDO::FETCH_ASSOC);
    foreach ($rows as $idx => $row) {
    	$uid = $row['user_id'];
    	$date = $row['date'];
    	$score = $row['score'];
    	if (!array_key_exists($date, $date_to_ranks)) {
    	    $date_to_ranks[$date] = new set([$score]);
    	    $update_users_dates($uid, $user_id, $date, $users_dates);
    	} elseif (
    	    $date_to_ranks[$date]->size() < $rank || $date_to_ranks[$date]->has($score)
    	) {
    	    $date_to_ranks[$date]->add($score);
    	    $update_users_dates($uid, $user_id, $date, $users_dates);
    	}
    }

    return $users_dates->toList();
}


function parse_dates_for_user_in_top_n($results, $user_id, $rank) {
    // Note: `$query_1` in fetch_data_ordered_by_date_score_query
    // returns desired results, but syntactically
    // appears to be incongruent with PDO/sqlite, hence the reason
    // for this high data storage method.
    $users_dates = new set([]);
    $update_users_dates = function($rid, $uid, $date, $list) {
    	if ($rid == $uid) {
    	    $list->add($date);
    	}
    };
    
    $date_to_ranks = [];
    $rows = $results->fetchAll(PDO::FETCH_ASSOC);
    foreach ($rows as $idx => $row) {
    	$uid = $row['user_id'];
    	$date = $row['date'];
    	$score = $row['score'];
    	if (!array_key_exists($date, $date_to_ranks)) {
    	    $date_to_ranks[$date] = ['rank' => 1, 'score' => $score];
    	    $update_users_dates($uid, $user_id, $date, $users_dates);
    	} elseif ($date_to_ranks[$date]['rank'] < $rank) {
    	    $date_to_ranks[$date]['rank']++;
    	    $update_users_dates($uid, $user_id, $date, $users_dates);
    	} elseif ($date_to_ranks[$date]['score'] == $score) {
    	    // This edge case doesn't make sense to me, but maybe I am
    	    // missing something obvious.
    	    // Seems that you'd either want to handle JUST the top
    	    // n scores (non-distinct) for a given date, OR
    	    // the top n distinct scores for a given date and THEN
    	    // determine whether the users exists within these score sets
    	    // for their respective dates.
    	    // Given the tests, I initially thought the later was
    	    // expected (see _parse_dates_for_user_in_top_n_alt above),
    	    // but that approach doesn't pass the tests.
    	    $update_users_dates($uid, $user_id, $date, $users_dates);
    	}
    }

	return $users_dates->toList();
}


function dates_when_user_was_in_top_n($pdo, $user_id, $rank) {
    try {
    	$results = $pdo->query(
    	    fetch_data_ordered_by_date_score_query($user_id, $rank)
    	);
    } catch (PDOException $e) {
        printf(
            'Failed to query results for',
            'dates_when_user_was_in_top_n due to exception: %s',
            $e->getMessage()
        );
        return false;
    }
    
    return parse_dates_for_user_in_top_n($results, $user_id, $rank);
}

