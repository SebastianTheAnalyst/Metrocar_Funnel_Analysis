-- Funnel Code

-- Step 1: Create a temporary table with user details including app downloads, signups, and ride requests.
with user_details AS (
    SELECT 
        app_download_key, 
        signups.user_id, 
        platform, 
        age_range, 
        ride_id,
        date(download_ts) AS download_date
    FROM 
        app_downloads
    LEFT JOIN 
        signups ON app_downloads.app_download_key = signups.session_id
    LEFT JOIN 
        ride_requests ON ride_requests.user_id = signups.user_id
),

-- Step 2: Count the number of downloads for each combination of platform, age range, and download date.
downloads AS (
    SELECT 
        0 as funnel_step,
        'download' as funnel_name,
        platform,
        age_range,
        download_date,
        COUNT (DISTINCT app_download_key) as users_count,
        0 as count_rides
    FROM 
        user_details
    GROUP BY 
        platform, age_range, download_date
),

-- Step 3: Count the number of signups for each combination of platform, age range, and download date.
signup AS (
    SELECT 
        1 as funnel_step,
        'signup' as funnel_name,
        user_details.platform,
        user_details.age_range,
        user_details.download_date,
        COUNT (DISTINCT signups.user_id) as users_count,
        0 as count_rides
    FROM 
        signups
    JOIN 
        user_details USING (user_id)
    WHERE 
        signup_ts is not null
    GROUP BY 
        user_details.platform, user_details.age_range, user_details.download_date
),

-- Step 4: Count the number of ride requests for each combination of platform, age range, and download date.
requested AS (
    SELECT 
        2 as funnel_step,
        'ride_requested' as funnel_name,
        user_details.platform,
        user_details.age_range,
        user_details.download_date,
        COUNT (DISTINCT user_id) as users_count,
        COUNT (DISTINCT ride_requests.ride_id) as count_rides
    FROM 
        ride_requests
    JOIN 
        user_details USING (user_id)
    WHERE 
        request_ts is not null
    GROUP BY 
        user_details.platform, user_details.age_range, user_details.download_date
),

-- Step 5: Count the number of ride acceptances for each combination of platform, age range, and download date.
accepted AS (
    SELECT 
        3 as funnel_step,
        'ride_accepted' as funnel_name,
        user_details.platform,
        user_details.age_range,
        user_details.download_date,
        COUNT (DISTINCT user_id) as users_count,
        COUNT (DISTINCT ride_requests.ride_id) as count_rides
    FROM 
        ride_requests
    JOIN 
        user_details USING (user_id)
    WHERE 
        accept_ts is not null
    GROUP BY 
        user_details.platform, user_details.age_range, user_details.download_date
),

-- Step 6: Count the number of completed rides for each combination of platform, age range, and download date.
completed AS (
    SELECT 
        4 as funnel_step,
        'ride_completed' as funnel_name,
        user_details.platform,
        user_details.age_range,
        user_details.download_date,
        COUNT (DISTINCT user_id) as users_count,
        COUNT (DISTINCT ride_requests.ride_id) as count_rides
    FROM 
        ride_requests
    JOIN 
        user_details USING (user_id)
    WHERE 
        dropoff_ts is not null
    GROUP BY 
        user_details.platform, user_details.age_range, user_details.download_date
),

-- Step 7: Count the number of payments for each combination of platform, age range, and download date.
payment AS (
    SELECT 
        5 as funnel_step,
        'payment' as funnel_name,
        user_details.platform,
        user_details.age_range,
        user_details.download_date,
        COUNT (DISTINCT user_id) AS users_count,
        COUNT (DISTINCT transactions.ride_id) as count_rides
    FROM 
        transactions
    JOIN 
        user_details USING (ride_id)
    WHERE 
        charge_status = 'Approved'
    GROUP BY 
        user_details.platform, user_details.age_range, user_details.download_date
),

-- Step 8: Count the number of reviews for each combination of platform, age range, and download date.
reviews AS (
    SELECT 
        6 as funnel_step,
        'review' as funnel_name,
        user_details.platform,
        user_details.age_range,
        user_details.download_date,
        COUNT (DISTINCT reviews.user_id) as users_count,
        COUNT (DISTINCT reviews.ride_id) as count_rides
    FROM 
        reviews
    JOIN 
        user_details USING (ride_id)
    GROUP BY 
        user_details.platform, user_details.age_range, user_details.download_date
)

-- Step 9: Combine the results from all the temporary tables using UNION.
SELECT *
FROM downloads
UNION
SELECT *
FROM signup
UNION
SELECT *
FROM requested
UNION
SELECT *
FROM accepted
UNION
SELECT *
FROM completed
UNION
SELECT *
FROM payment
UNION
SELECT *
FROM reviews

-- Step 10: Order the results by funnel step, platform, age range, and download date.
ORDER BY funnel_step, platform, age_range, download_date;
