use Mix.Config

config :adjust, :database,
  hostname: "localhost",
  username: "postgres",
  password: "postgres",
  database: "postgres" # <- this database is used at first connect to create working databases

config :logger, level: :info
