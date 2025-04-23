LLMexcel is a command-line tool that lets users interact with a local SQLite database using both direct SQL queries and natural language prompts. It integrates OpenAI's GPT model to generate SQL queries from plain English, making it easier to explore and query data.

## Features

- Load CSV files into a SQLite database
- List all tables in the database
- Execute raw SQL queries
- Generate SQL queries from natural language using GPT-4
- Handle schema conflicts when loading new tables

## Requirements

- Python 3.8+
- OpenAI API Key
Set your OpenAI API key as an environment variable:

On macOS/Linux:

export OPENAI_API_KEY=your-api-key
On Windows:

set OPENAI_API_KEY=your-api-key
