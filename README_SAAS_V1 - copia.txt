MARGAY SAAS V1

Cambios principales:
- Login por veterinaria
- Tabla empresas
- Tabla usuarios
- empresa_id agregado a tablas principales
- Usuario migrado por defecto para la data existente:
  veterinaria: Margay
  email: admin@margay.local
  clave: admin1234

Para correr:
1) instalar dependencias Flask y Werkzeug
2) ejecutar: python app.py
3) abrir /login

Notas:
- Esta v1 es la base multiempresa.
- La app original se reutiliza; no se rehizo desde cero.
- Ya queda pronta para seguir a v2 con planes, alta de clínicas y hosting.
