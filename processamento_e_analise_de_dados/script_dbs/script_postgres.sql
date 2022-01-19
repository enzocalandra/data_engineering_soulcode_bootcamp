

create table cidades_ibge(
    id serial not null primary key,
    uf varchar(10),
    cod_uf integer,
    cod_munic integer,
    nome_do_municipio varchar(100),
    populacao_estimada varchar(50)
);


create table dados_ibge(
    id serial not null primary key,
    ano integer,
    id_municipio integer,
    pib bigint,
    impostos_liquidos bigint,
    va bigint,
    va_agropecuaria bigint,
    va_industria bigint,
    va_servicos bigint,
    va_adespss bigint
);

create table reclamacoes(
    id serial not null primary key,
    dataextracao varchar(100),
    solicitacoes integer,
    ano integer,
    mes integer,
    anomes varchar(100), 
    uf varchar(2),
    cidade varchar(100),
    co_municipio integer,  
    canalentrada varchar(100),   
    condicoes varchar(100),
    tipoatendimento varchar(100),
    servico varchar(100),
    marca varchar(100), 
    assunto text,
    problema text
);



create table qualidade(
    id serial not null primary key,
    servico varchar(100),
    sigla_do_servico varchar(100),
    tipo_de_outorga varchar(100),
    empresa varchar(100), 
    razao_social varchar(100),
    cnpj bigint,
    uf varchar(100), 
    area_de_calculo_do_indicador varchar(100), 
    indicador varchar(100),
    periodo_de_coleta text,
    meta_do_indicador text, 
    mes_ano varchar(50),
    ano integer,
    grupo_do_indicador varchar(100),
    resultados varchar(100), 
    descumpriu integer,
    cumpriu integer,
    no integer, 
    ni integer, 
    total integer,
    normativo varchar(100) 
);



create table cobertura_operadoras(
    id serial not null primary key,
    ano integer, 
    operadora varchar(100),
    tecnologia varchar(100), 
    codigo_setor_censitario bigint,
    bairro varchar(100), 
    tipo_setor varchar(100),
    codigo_localidade bigint,
    nome_localidade varchar(100),      
    categoria_localidade varchar(100),
    localidade_agregadora varchar(100),   
    codigo_municipio integer,  
    municipio varchar(100),
    uf varchar(2),
    regiao varchar(100),
    area_km2 varchar(100),
    domicilios integer,
    moradores integer ,
    percentual_cobertura varchar(100)
);



CREATE TABLE IF NOT EXISTS registro_log(
     cod_log serial not null primary key,
     usuario varchar(50),
     data_insert TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
     descricao_log text
);


-- CRIANDO FUNCTION E TRIGGERS PARA INSERIR LOGS DE REGISTRO DE OPERAÇÕES REALIZADAS

create or replace function tgr_function_registro_log()
returns trigger as $$
    begin
        if (TG_OP = 'INSERT') then 
            insert into registro_log(usuario, descricao_log)
            values (current_user, ' Operação de inserção: '|| new.* || ' .');
            return new;
        ELSIF (TG_OP = 'UPDATE') then
            insert into registro_log(usuario, descricao_log)
            values (current_user, ' Operação de update: ' 
                    || old.* || ' para ' || new.* || ' .');
            return new;
        ELSIF (TG_OP = 'DELETE') then
            insert into registro_log(usuario, descricao_log)
            values (current_user, ' Operação de delete: '|| old.* || ' .');
                return old;
        end if;
        return null;
    end;
$$
language 'plpgsql';


-- DROP TRIGGER trg_cidades_ibge ON  cidades_ibge;

create trigger trg_cidades_ibge
    after insert or update or delete on cidades_ibge
        for each row 
            execute procedure tgr_function_registro_log();

create trigger trg_dados_ibge
    after insert or update or delete on dados_ibge
        for each row 
            execute procedure tgr_function_registro_log();

create trigger trg_reclamacoes
    after insert or update or delete on reclamacoes
        for each row 
            execute procedure tgr_function_registro_log();
            
create trigger trg_qualidade
    after insert or update or delete on qualidade
        for each row 
            execute procedure tgr_function_registro_log();


create trigger trg_cobertura_operadoras
    after insert or update or delete on cobertura_operadoras
        for each row 
            execute procedure tgr_function_registro_log();



insert into cidades_ibge (uf, cod_uf, cod_munic, nome_do_municipio, populacao_estimada) values ('sc', 123, 52212, 'Joinville', '390000');
insert into dados_ibge( ano, id_municipio, pib, impostos_liquidos, va, va_agropecuaria, va_industria, va_servicos, va_adespss) values(2020, 123, 123456, 12313211, 123132, 2313213, 13213213, 151615, 84946156);
insert into reclamacoes(dataextracao, solicitacoes, ano, mes, anomes, uf, cidade, co_municipio, canalentrada, condicoes, tipoatendimento, servico , marca, assunto, problema) values ('2020-01-01', 121, 2020, 01, '2020-01', 'sc', 'Criciuma', 123, 'canalentrada', 'condicoes)', 'tipoatendimento', 'servico', 'marca', 'assunto', 'problema');
insert into qualidade(servico, sigla_do_servico, tipo_de_outorga, empresa, razao_social, cnpj, uf, area_de_calculo_do_indicador, indicador, periodo_de_coleta, meta_do_indicador, mes_ano, ano, grupo_do_indicador, resultados, descumpriu, cumpriu, no, ni, total, normativo) values ('servico', 'sigla', 'tipo', 'empresa', 'razao', 123456789789, 'uf', 'area de calculo', 'indicador', 'periodo', 'metadoind', '10/2020', 2020, 'grupo_do_indicador', 'resultados', 123, 123, 123, 123, 123456, 'normativo');
insert into cobertura_operadoras(ano, operadora, tecnologia, codigo_setor_censitario, bairro, tipo_setor, codigo_localidade, nome_localidade, categoria_localidade, localidade_agregadora, codigo_municipio, municipio, uf, regiao, area_km2, domicilios, moradores, percentual_cobertura) values(2021, 'operadora', 'tecnologia varchar(100)', 123465, 'bairro varchar(100)', 'tipo_setor varchar(100)', 456789131, 'nome_localidade varchar(100)', 'categoria_localidade varchar(100)', 'localidade_agregadora varchar(100)', 123, 'municipio varchar(100)', 'uf', 'regiao varchar(100)', 'area_km2 varchar(100)', 132223, 1356487 , 'percentual_cobertura varchar(100)');


select * from registro_log;