# PostgresDataFetch
This project demonstrates a FastAPI application that connects to a PostgreSQL database hosted on Neon.tech. The API allows fetching and adding users to the database using efficient asynchronous operations. It also showcases the use of Polars for handling data with LazyFrame for deferred computations.
1) GET / : Fetches all users lazily from the PostgreSQL database using Polars LazyFrame. The endpoint returns the collected data as JSON.
2) POST /: Adds one or more users consisting of name and email address to the PostgreSQL database.
