
//Q1 reviews having word twist OR  spoiler keyword

db.getCollection("imdbreviews").find(
    { 
        "$or" : [
            { 
                "review_text" : /^.*spoiler.*$/i
                
            }, 
            { 
                "review_text" : /^.*twist.*$/i
            }
        ]
    }
//);





//Q2 users with atleast one spoiler
db.getCollection("imdbreviews").find(
    {
        "is_spoiler" : true
    },
    {
        "user_id" : "$user_id",
        "review_text" : "$review_text",
        "_id" : NumberInt(0)
    }
);


//Q3 Most spoilers for imdb rating 10: Total spoilers:150924,30599 FOR 10 RATING

db.getCollection("imdbreviews").find({ "is_spoiler" : true, "rating" : "10"}).count()



//Q4Query to see for which duration of the movies that have most of the spoilers.
db.getCollection("imdbreviews").aggregate(
    [
        { 
            "$project" : { 
                "_id" : NumberInt(0), 
                "imdbreviews" : "$$ROOT"
            }
        }, 
        { 
            "$lookup" : { 
                "localField" : "imdbreviews.movie_id", 
                "from" : "imdbmoviedetails", 
                "foreignField" : "movie_id", 
                "as" : "imdbmoviedetails"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$imdbmoviedetails", 
                "preserveNullAndEmptyArrays" : false
            }
        }, 
        { 
            "$match" : { 
                "imdbreviews.is_spoiler" : true
            }
        }, 
        { 
            "$project" : { 
                "imdbmoviedetails.duration" : "$imdbmoviedetails.duration", 
                "imdbreviews.is_spoiler" : "$imdbreviews.is_spoiler", 
                "_id" : NumberInt(0)
            }
        }
    ], 
    { 
        "allowDiskUse" : true
    }
);






//Q5Movie with most spoiler reviews 
db.getCollection("imdbreviews").aggregate(
    [
        { 
            "$match" : { 
                "is_spoiler" : true
            }
        }, 
        { 
            "$group" : { 
                "_id" : { 
                    "movie_id" : "$movie_id"
                }, 
                "COUNT(*)" : { 
                    "$sum" : NumberInt(1)
                }
            }
        }, 
        { 
            "$project" : { 
                "movie_id" : "$_id.movie_id", 
                "COUNT(*)" : "$COUNT(*)", 
                "_id" : NumberInt(0)
            }
        }, 
        { 
            "$sort" : { 
                "COUNT(*)" : NumberInt(-1)
            }
        }, 
        { 
            "$limit" : NumberInt(1)
        }
    ], 
    { 
        "allowDiskUse" : true
    }
);


//Q6 Users who has posted most spoiler reviews

db.getCollection("imdbreviews").aggregate(
    [
        { 
            "$match" : { 
                "is_spoiler" : true
            }
        }, 
        { 
            "$group" : { 
                "_id" : { 
                    "user_id" : "$user_id"
                }, 
                "COUNT(*)" : { 
                    "$sum" : NumberInt(1)
                }
            }
        }, 
        { 
            "$project" : { 
                "user_id" : "$_id.user_id", 
                "COUNT(*)" : "$COUNT(*)", 
                "_id" : NumberInt(0)
            }
        }, 
        { 
            "$sort" : { 
                "COUNT(*)" : NumberInt(-1)
            }
        }, 
        { 
            "$limit" : NumberInt(1)
        }
    ], 
    { 
        "allowDiskUse" : true
    }
);

//Q.7 how fast the spoiler is declared in review after release date
db.getCollection("imdbmoviedetails").aggregate(
    [
        {
            "$project" : {
                "_id" : NumberInt(0),
                "imdbmoviedetails" : "$$ROOT"
            }
        },
        {
            "$lookup" : {
                "localField" : "imdbmoviedetails.movie_id",
                "from" : "imdbreviews",
                "foreignField" : "movie_id",
                "as" : "imdbreviews"
            }
        },
        {
            "$unwind" : {
                "path" : "$imdbreviews",
                "preserveNullAndEmptyArrays" : false
            }
        },
        {
            "$match" : {
                "imdbreviews.is_spoiler" : true
            }
        },
        {
            "$project" : {
                "imdbmoviedetails.release_date" : "$imdbmoviedetails.release_date",
                "imdbreviews.review_date" : "$imdbreviews.review_date",
                "imdbreviews.is_spoiler" : "$imdbreviews.is_spoiler",
                "_id" : NumberInt(0)
            }
        }
    ],
    {
        "allowDiskUse" : true
    }
);
//Q.8  genre having most spoiler reviews
db.getCollection("imdbreviews").aggregate(
        [
        { 
            "$project" : { 
                "_id" : NumberInt(0), 
                "imdbreviews" : "$$ROOT"
            }
        }, 
        { 
            "$lookup" : { 
                "localField" : "imdbreviews.movie_id", 
                "from" : "imdbmoviedetails", 
                "foreignField" : "movie_id", 
                "as" : "imdbmoviedetails"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$imdbmoviedetails", 
                "preserveNullAndEmptyArrays" : false
            }
        }, 
        { 
            "$match" : { 
                "imdbreviews.is_spoiler" : true
            }
        }, 
        { 
            "$project" : { 
                "imdbmoviedetails.genre" : "$imdbmoviedetails.genre", 
                "imdbreviews.is_spoiler" : "$imdbreviews.is_spoiler", 
                "_id" : NumberInt(0)
            }
        }
    ], 
    { 
        "allowDiskUse" : true
    }
);



















