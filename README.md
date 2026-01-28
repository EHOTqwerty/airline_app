# airline_app
Praca dyplomowa "Opracowanie aplikacji bazodanowej do obsługi linii lotniczej z użyciem języka programowania Python "

Aplikacja zostala napisana w Visual Studio Code

Do zrobienia w ten moment:
Skonczyc "Ceny vs Ryzyko"
Dodac 2 nowy raporty

Potrzebne:
Python
Xampp

Po pobraniu:
python -m venv .venv
pip install -r requirements.txt
wstawic do phpmyadmin -> sql polecenia app/sql/001_create_db.sql oraz app/sql/002_schema.sql

Przed rozpoczeciem pracy:
.venv/Scripts/Activate.ps1

Uruchomienie programu:
python gui.py


Problem z aktywacja venv:
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned

Problem z pip:
python -m pip install -r requirements.txt

Pobieranie z github:
git clone https://github.com/EHOTqwerty/airline_app.git
cd airline_app
code .

Pobranie z nadpisaniem:
git fetch origin
git reset --hard origin/main
git clean -fd

Zapisanie na github:
git status
git add .
git commit -m "Blablabla"
git push origin main

Dodatkowe:
git status