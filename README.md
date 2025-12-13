
# GithubDb

This is webscraper-in-a-library that attempts to build a synchronized view of a Github repository's metadata.
In other words, it generates and keeps up to date a databse of comments, pull requests and issues.
By being careful with how data is cached, it can do so for very large repositories 
(see `examples/rust` for a scraper that indexes <https://github.com/rust-lang/rust>)
on a single api token.
The resulting database can then be used to run arbitrary queries on that might be very expensive
to compute when scraping the data just for that query from Github directly. 

## Warning

This gathers all information from a repo without consent of contributors to said repository.
Even if the repository is public, this may be ethically questionable.
I ask you to use this software responsibly. Please.

## Shout out

This library uses [rust-query](https://github.com/lholten/rust-query), 
an awesome database library created by a friend of mine.
Give it a try if you get the chance!
