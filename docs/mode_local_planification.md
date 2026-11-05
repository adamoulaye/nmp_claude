# Planification de l'archivage et des controles

Linux/macOS (crontab -e), toutes les 30 min pour l'archivage et 5 min pour les SLO:

    */30 * * * * cd /chemin/projet && .venv/bin/python scripts/archive_raw.py --lookback-hours 6 >> data/logs/cron.log 2>&1
    */5  * * * * cd /chemin/projet && .venv/bin/python scripts/check_slos.py >> data/logs/cron.log 2>&1

Windows: Planificateur de taches, declencheur "toutes les 30 minutes", action = python.exe + chemin du script.
Un portable en veille NE collecte PAS: preferez une petite VM Azure, un Azure Function (timer trigger)
ou un job Databricks planifie des que le pipeline est stable.
