module.exports = {
  apps: [
    {
      name: "callio-etl-0930",
      script: "callio_etl/__main__.py",
      interpreter: "python",
      args: "--mode once --job all",
      cwd: __dirname,
      env_file: ".env",
      env: {
        PYTHONUNBUFFERED: "1",
      },
      cron: "30 9 * * *",
      cron_tz: "Asia/Ho_Chi_Minh",
      autorestart: false,
      log_date_format: "YYYY-MM-DD HH:mm:ss",
    },
    {
      name: "callio-etl-slots",
      script: "callio_etl/__main__.py",
      interpreter: "python",
      args: "--mode once --job all",
      cwd: __dirname,
      env_file: ".env",
      env: {
        PYTHONUNBUFFERED: "1",
      },
      cron: "0 11,13,15,18 * * *",
      cron_tz: "Asia/Ho_Chi_Minh",
      autorestart: false,
      log_date_format: "YYYY-MM-DD HH:mm:ss",
    },
  ],
};
