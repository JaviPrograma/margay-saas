MARGAY SAAS - LISTO PARA RENDER

1) Subi esta carpeta a un repo de GitHub.
2) En Render: New > Web Service > Connect repository.
3) Si Render detecta render.yaml, deja esa config.
4) Si te pide manualmente:
   Build Command: pip install -r requirements.txt
   Start Command: python app.py
5) La base SQLite (veterinaria.db) sirve para pruebas, pero en Render free es efimera.
   Si redeployas o reinicias, algunos cambios pueden perderse.
6) Usuario inicial:
   email: admin@margay.local
   clave: admin1234
7) Para crear veterinarias nuevas: iniciar sesion > Veterinarias.
