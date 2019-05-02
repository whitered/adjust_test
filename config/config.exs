# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

config :adjust, :database,
  hostname: "localhost",
  username: "postgres",
  password: "postgres",
  database: "postgres"
