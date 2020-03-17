CREATE DATABASE dictas;
CREATE USER dictas WITH ENCRYPTED PASSWORD 'dictas01';
GRANT ALL PRIVILEGES ON DATABASE dictas TO dictas;
\connect dictas;

CREATE SCHEMA IF NOT EXISTS dictas AUTHORIZATION dictas;

-- ************************************** "dictas"."bt_prestador"

CREATE TABLE "dictas"."bt_prestador"
(
 "pk_medico_prestador" int NOT NULL,
 "nome_fantasia"       varchar(250) NULL,
 "cod_crm"             int NULL,
 "uf_crm"              varchar(2) NULL,
 "num_cpf_cnpj"        varchar(200) NULL,
 "tipo_prestador"      varchar(100) NULL,
 "grupo_prestador"     text NULL,
 "especialidade"       varchar(50) NULL,
 "grau"                int NULL

);

CREATE UNIQUE INDEX "PK_Prestador" ON "dictas"."bt_prestador"
(
 "pk_medico_prestador"
);

-- ************************************** "dictas"."bt_empresa"

CREATE TABLE "dictas"."bt_empresa"
(
 "pk_empresa" int NOT NULL,
 "nome"       varchar(250) NULL,
 "cnpj"       varchar(50) NULL

);

CREATE UNIQUE INDEX "PK_eDictasSageEmpresa" ON "dictas"."bt_empresa"
(
 "pk_empresa"
);

-- ************************************** "dictas"."bt_servico"

CREATE TABLE "dictas"."bt_servico"
(
 "pk_servico"    bigint NOT NULL,
 "tipo"          varchar(50) NULL,
 "descricao"     text NULL,
 "capitulo"      text NULL,
 "grupo"         text NULL,
 "subgrupo"      text NULL,
 "ind_cirurgico" char(1) NULL

);

CREATE UNIQUE INDEX "PK_Servico" ON "dictas"."bt_servico"
(
 "pk_servico"
);

-- ************************************** "dictas"."bt_beneficiario"

CREATE TABLE "dictas"."bt_beneficiario"
(
 "pk_beneficiario"        int NOT NULL,
 "nr_beneficiario"        bigint NULL,
 "nr_beneficiario_tit"    bigint NULL,
 "fk_empresa"             int NOT NULL,
 "nome"                   varchar(250) NULL,
 "cpf"                    varchar(50) NULL,
 "dt_nascimento"          date NULL,
 "cod_plano"              bigint NULL,
 "descricao_plano"        text NULL,
 "copart_percentual"      decimal(15,2) NULL,
 "tipo_acomodacao"        varchar(50) NULL,
 "abrangencia_plano"      varchar(50) NULL,
 "grau_dependencia"       varchar(50) NULL,
 "dt_inclusao"            date NULL,
 "dt_exclusao"            date NOT NULL,
 "sexo"                   char(1) NULL,
 "tipo_contrato"          varchar(50) NULL,
 "nr_cartaonacionalsaude" bigint NULL,
 "descricao_entidade"     text NULL,
 CONSTRAINT "FK_158" FOREIGN KEY ( "fk_empresa" ) REFERENCES "dictas"."bt_empresa" ( "pk_empresa" )
);

CREATE UNIQUE INDEX "PK_Beneficiario" ON "dictas"."bt_beneficiario"
(
 "pk_beneficiario"
);

CREATE INDEX "fkIdx_158" ON "dictas"."bt_beneficiario"
(
 "fk_empresa"
);

-- ************************************** "dictas"."bt_vidas"

CREATE TABLE "dictas"."bt_vidas"
(
 "cd_mes_competencia" int NULL,
 "fk_beneficiario"    int NOT NULL,
 "de_ativo"           int NULL,
 CONSTRAINT "FK_176" FOREIGN KEY ( "fk_beneficiario" ) REFERENCES "dictas"."bt_beneficiario" ( "pk_beneficiario" )
);

CREATE INDEX "fkIdx_176" ON "dictas"."bt_vidas"
(
 "fk_beneficiario"
);

-- ************************************** "dictas"."bt_receita"

CREATE TABLE "dictas"."bt_receita"
(
 "cd_mes_competencia" int NULL,
 "fk_beneficiario"    int NOT NULL,
 "fk_empresa"         int NOT NULL,
 "dt_geracao_titulo"  date NULL,
 "tipo_cobranca"      varchar(100) NULL,
 "vl_cobranca"        decimal(15,2) NULL,
 "vl_pago"            decimal(15,2) NULL,
 CONSTRAINT "FK_167" FOREIGN KEY ( "fk_empresa" ) REFERENCES "dictas"."bt_empresa" ( "pk_empresa" ),
 CONSTRAINT "FK_170" FOREIGN KEY ( "fk_beneficiario" ) REFERENCES "dictas"."bt_beneficiario" ( "pk_beneficiario" )
);

CREATE INDEX "fkIdx_167" ON "dictas"."bt_receita"
(
 "fk_empresa"
);

CREATE INDEX "fkIdx_170" ON "dictas"."bt_receita"
(
 "fk_beneficiario"
);

-- ************************************** "dictas"."bt_custo"

CREATE TABLE "dictas"."bt_custo"
(
 "fk_beneficiario"      int NOT NULL,
 "mes_competencia"      int NULL,
 "nr_guia"              int NULL,
 "nr_guia_principal"    int NULL,
 "dt_realizacao"        date NULL,
 "fk_cid"               int NOT NULL,
 "dt_ini_internacao"    date NULL,
 "dt_fim_internacao"    date NULL,
 "fk_medicosolicitante" int NOT NULL,
 "cod_executante"       int NULL,
 "fk_local_atend"       int NOT NULL,
 "fk_capa_unico"        int NOT NULL,
 "fk_destino_pgto"      int NOT NULL,
 "fk_servico"           bigint NOT NULL,
 "cod_servico_tipo"     int NULL,
 "qtd"                  int NULL,
 "qtd_unica"            int NULL,
 "vl_unitario"          decimal(15,2) NULL,
 "vl_total"             decimal(15,2) NULL,
 "vl_coparticipacao"    decimal(15,2) NULL,
 "ind_internacao"       boolean NULL,
 "tipo_registro"        char(1) NULL,
 "tipo_atendimento"     varchar(50) NULL,
 "tipo_guia"            varchar(50) NULL,
 "tipo_internacao"      varchar(50) NULL,
 "tipo_acomodacao"      varchar(50) NULL,
 "vl_hm"                decimal(15,2) NULL,
 "vl_co"                decimal(15,2) NULL,
 "vl_filme"             decimal(15,2) NULL,
 "partic_medico"        decimal(15,2) NULL,
 CONSTRAINT "FK_187" FOREIGN KEY ( "fk_beneficiario" ) REFERENCES "dictas"."bt_beneficiario" ( "pk_beneficiario" ),
 CONSTRAINT "FK_190" FOREIGN KEY ( "fk_servico" ) REFERENCES "dictas"."bt_servico" ( "pk_servico" ),
 CONSTRAINT "FK_193" FOREIGN KEY ( "fk_medicosolicitante" ) REFERENCES "dictas"."bt_prestador" ( "pk_medico_prestador" )
);


CREATE INDEX "fkIdx_187" ON "dictas"."bt_custo"
(
 "fk_beneficiario"
);

CREATE INDEX "fkIdx_190" ON "dictas"."bt_custo"
(
 "fk_servico"
);

CREATE INDEX "fkIdx_193" ON "dictas"."bt_custo"
(
 "fk_medicosolicitante"
);













































































