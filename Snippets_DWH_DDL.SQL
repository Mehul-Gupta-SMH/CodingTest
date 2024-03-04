CREATE TABLE snippets_dwh (
    id SERIAL PRIMARY KEY,
    snippet TEXT NOT NULL,
    language TEXT NOT NULL,
    repo_file_name TEXT,
    github_repo_url TEXT,
    license TEXT,
    commit_hash TEXT,
    starting_line_number INTEGER,
    chunk_size INTEGER,
    modifieddate timestamp not null default Current_timestamp,
    CONSTRAINT unique_commit_repo_url_chunk_start_ UNIQUE(commit_hash, repo_file_name, github_repo_url, chunk_size, starting_line_number)
);

CREATE INDEX snippets_dwh_github_repo_url ON snippets_dwh(github_repo_url);
CREATE INDEX snippets_dwh_license ON snippets_dwh(license);
CREATE INDEX snippets_dwh_language ON snippets_dwh(language);
