CREATE TABLE CustReviews_DWH (
    overall INT,
    vote INT,
    verified BOOLEAN,
    reviewTime DATE,
    reviewerID VARCHAR(20),
    "asin" VARCHAR(20),
    "style" Text,
    reviewerName VARCHAR(200),
    reviewText TEXT,
    summary Text,
    unixReviewTime Text,
    image TEXT,
    modifieddate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
