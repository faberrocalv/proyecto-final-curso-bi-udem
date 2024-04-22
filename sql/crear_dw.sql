CREATE TABLE "ventas" (
  "id_factura" varchar,
  "id_cliente" varchar,
  "id_categoria" integer,
  "id_metodo_pago" integer,
  "id_fecha" integer,
  "id_centro_comercial" integer,
  "precio_unitario" float,
  "cantidad" integer,
  "venta_total" float
);

CREATE TABLE "clientes" (
  "id" varchar PRIMARY KEY,
  "genero" varchar,
  "edad" integer
);

CREATE TABLE "metodo_pago" (
  "id" integer PRIMARY KEY,
  "nombre_metodo_pago" varchar
);

CREATE TABLE "fecha" (
  "id" integer PRIMARY KEY,
  "fecha_completa" date,
  "anio" integer,
  "mes" integer,
  "dia" integer,
  "trimestre" integer,
  "nombre_mes" varchar,
  "dia_semana" varchar
);

CREATE TABLE "categoria" (
  "id" integer PRIMARY KEY,
  "nombre_categoria" varchar
);

CREATE TABLE "centro_comercial" (
  "id" integer PRIMARY KEY,
  "nombre_centro_comercial" varchar
);

ALTER TABLE "ventas" ADD FOREIGN KEY ("id_cliente") REFERENCES "clientes" ("id");

ALTER TABLE "ventas" ADD FOREIGN KEY ("id_metodo_pago") REFERENCES "metodo_pago" ("id");

ALTER TABLE "ventas" ADD FOREIGN KEY ("id_fecha") REFERENCES "fecha" ("id");

ALTER TABLE "ventas" ADD FOREIGN KEY ("id_categoria") REFERENCES "categoria" ("id");

ALTER TABLE "ventas" ADD FOREIGN KEY ("id_centro_comercial") REFERENCES "centro_comercial" ("id");
