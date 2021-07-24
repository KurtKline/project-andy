# project-andy
Importing csv files and loading them into a PostgreSQL database

## Docker / PostgreSQL installation
In order for this script to work, you need to have a PostgreSQL database on your local machine. 

I followed along with this resource: https://dev.to/shree_j/how-to-install-and-run-psql-using-docker-41j2

When installing postgres (step #2 below), I used a recommended version from the comments. That is because 
even if the Docker container is stopped and restarted, the data will persist. In the original instructions 
the data would be lost if the container is stopped. 

1. Install Docker
2. Install PostgreSQL image and start container

```
sudo docker run \
  -d \
  --name postgresql-container \
  -p 5432:5432 \
  -e POSTGRES_PASSWORD=kvEHgo1943bf \
  -v /home/kurt/pgdata:/var/lib/postgresql/data \
  postgres
```

3. Create pgAdmin container
```
docker run --rm -p 5050:5050 thajeztah/pgadmin4
```

4. Access pgAdmin
Go to browser and type in ```localhost:5050```.

Note: You can only access pgAdmin if the pdAdmin docker container is running

## Adding cron job to schedule your script

1. In the Linux terminal, type `crontab -e`. This will open a file 
where you can add new cron jobs. 

2. Add your cron job in the following format: 
```
*/1 * * * * <PATH_TO_PYTHON_IN_VENV> <PATH_TO_SCRIPT>

So in my case it was: 
*/1 * * * * /home/kurt/Git/ProjectAndy/venv/bin/python /home/kurt/Git/ProjectAndy/main.py
```
The website [crontab.guru](https://crontab.guru/) can help you understand the scheduling better.

3. Save the file
4. Look in the projects `logs` folder to see if anything has happened. There should be a file 
  `run.log` that you can look at.
