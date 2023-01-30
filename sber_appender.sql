USE [BAR]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [Sber].[CSV_Appender]
AS
BEGIN

    --ПОЛУЧЕНИЕ СПИСКА ФАЙЛОВ ФАЙЛЕ--
    IF (OBJECT_ID('@opener') IS NULL)
        DECLARE @opener NVARCHAR(MAX);
    IF (OBJECT_ID('@k') IS NULL)
        DECLARE @k SMALLINT;
    IF (OBJECT_ID('@dirPath') IS NULL)
        DECLARE @dirpath NVARCHAR(500);
    IF (OBJECT_ID('@csvfile') IS NULL)
        DECLARE @csvfile NVARCHAR(500);
    IF (OBJECT_ID('@actdate') IS NULL)
        DECLARE @actdate NVARCHAR(12);
    IF (OBJECT_ID('@filepath') IS NULL)
        DECLARE @filepath NVARCHAR(500);
    SET @dirPath = 'C:\database\sber';
    CREATE TABLE Sber.Filenames
    (
        title nvarchar(500),
        depth int,
        isFile int
    );
    INSERT INTO Sber.Filenames
    EXEC xp_DirTree @dirPath, 1, 1;
    ALTER TABLE Sber.Filenames DROP COLUMN depth, isFile;

    --ФОРМИРОВАНИЕ ПУТИ К CSV-ФАЙЛУ С НАИБОЛЕЕ РАННЕЙ ДАТОЙ--
    SET @csvfile = STUFF(
                   (
                       SELECT TOP 1
                           title
                       FROM Sber.Filenames
                       WHERE title LIKE '%.csv'
                       ORDER BY CAST(CAST(LEFT(RIGHT(title, 12), 8) AS VARCHAR(8)) AS DATE) DESC
                   ),
                   1,
                   0,
                   ''
                        );
    SET @actdate
        = LEFT(STUFF(
               (
                   SELECT TOP 1
                       CAST(DATEADD(MONTH, -1, CAST(CAST(LEFT(RIGHT(title, 12), 8) AS VARCHAR(8)) AS DATE)) AS NVARCHAR(12)) --Сохранение актуального временного разреза
                   FROM Sber.Filenames
                   WHERE title LIKE '%.csv'
                   ORDER BY CAST(CAST(LEFT(RIGHT(title, 12), 8) AS VARCHAR(8)) AS DATE) DESC
               ),
               1,
               0,
               ''
                    ), 8) + '01';
    SET @filepath = @dirPath + '\' + @csvfile;

    --ЧТЕНИЕ ФАЙЛА ПРО ТРАНЗАКЦИИ ФИЗИЧЕСКИХ ЛИЦ---
    IF CHARINDEX('fl_trans', @csvfile) > 0
    BEGIN
        CREATE TABLE Sber.Upload_csv
        ( --Cоздание схемы таблицы
            mcc NVARCHAR(75) NULL,
            region_name NVARCHAR(35) NULL,
            ao_name NVARCHAR(75) NULL,
            district_name NVARCHAR(59) NULL,
            gender NVARCHAR(1) NULL,
            age_group SMALLINT NULL,
            income_group INT NULL,
            client_cnt_in_cluster INT NULL,
            trans_amount_on_terr NUMERIC(19, 2) NULL,
            trans_amount_other_terr NUMERIC(19, 2) NULL,
            trans_amount_other_region NUMERIC(19, 2) NULL,
            trans_amount_other_countries NUMERIC(19, 2) NULL,
            trans_amount_onl_rf NUMERIC(19, 2) NULL,
            trans_amount_onl_other NUMERIC(19, 2) NULL,
            count_trans_on_terr BIGINT NULL,
            count_trans_other_terr BIGINT NULL,
            count_trans_other_region BIGINT NULL,
            count_trans_other_countries BIGINT NULL,
            count_trans_onl_rf BIGINT NULL,
            count_trans_onl_other BIGINT NULL,
            client_cnt_on_terr BIGINT NULL,
            client_cnt_other_terr BIGINT NULL,
            client_cnt_other_region BIGINT NULL,
            client_cnt_other_countries BIGINT NULL,
            client_cnt_onl_rf BIGINT NULL,
            client_cnt_onl_other BIGINT NULL,
            partition_dt NVARCHAR(10) NULL
        );
        SELECT @opener
            = 'BULK INSERT Sber.Upload_csv FROM ' + quotename(@filepath, '''')
              + --Считывание файла с диска
            'WITH (FIELDTERMINATOR = '';'',
                   ROWTERMINATOR = ''0x0A'',
                   CODEPAGE = ''65001'',
                   FIRSTROW = 2,
                   FIELDQUOTE = ''"'',
                   FORMAT = ''CSV'')';
        EXEC sp_executesql @opener;

        --ПРОВЕРКА ТАБЛИЦЫ ПРО ТРАНЗАКЦИИ ФИЗИЧЕСКИХ ЛИЦ--
        SET @k = 0; --Cчётчик несоответствий критериев
        IF EXISTS
        (
            SELECT SUM(trans_amount_on_terr) AS trans_amount_on_terr,
                   SUM(trans_amount_onl_other) AS trans_amount_onl_other, --Гиперагрегация | Текстовые группы
                   SUM(trans_amount_onl_rf) AS trans_amount_onl_rf,
                   SUM(trans_amount_other_countries) AS trans_amount_other_countries,
                   SUM(trans_amount_other_region) AS trans_amount_other_region,
                   SUM(trans_amount_other_terr) AS trans_amount_other_terr,
                   SUM(count_trans_on_terr) AS count_trans_on_terr,
                   SUM(count_trans_onl_other) AS count_trans_onl_other,
                   SUM(count_trans_onl_rf) AS count_trans_onl_rf,
                   SUM(count_trans_other_countries) AS count_trans_other_countries,
                   SUM(count_trans_other_region) AS count_trans_other_region,
                   SUM(count_trans_other_terr) AS count_trans_other_terr,
                   SUM(client_cnt_on_terr) AS client_cnt_on_terr,
                   SUM(client_cnt_onl_other) AS client_cnt_onl_other,
                   SUM(client_cnt_onl_rf) AS client_cnt_onl_rf,
                   SUM(client_cnt_other_countries) AS client_cnt_other_countries,
                   SUM(client_cnt_other_region) AS client_cnt_other_region,
                   SUM(client_cnt_other_terr) AS client_cnt_other_terr
            FROM Sber.Upload_csv
            WHERE mcc IS NULL
                  AND [partition_dt] = @actdate --Сумма гиперагрегации
            EXCEPT
            SELECT SUM(trans_amount_on_terr) AS trans_amount_on_terr,
                   SUM(trans_amount_onl_other) AS trans_amount_onl_other,
                   SUM(trans_amount_onl_rf) AS trans_amount_onl_rf,
                   SUM(trans_amount_other_countries) AS trans_amount_other_countries,
                   SUM(trans_amount_other_region) AS trans_amount_other_region,
                   SUM(trans_amount_other_terr) AS trans_amount_other_terr,
                   SUM(count_trans_on_terr) AS count_trans_on_terr,
                   SUM(count_trans_onl_other) AS count_trans_onl_other,
                   SUM(count_trans_onl_rf) AS count_trans_onl_rf,
                   SUM(count_trans_other_countries) AS count_trans_other_countries,
                   SUM(count_trans_other_region) AS count_trans_other_region,
                   SUM(count_trans_other_terr) AS count_trans_other_terr,
                   SUM(client_cnt_on_terr) AS client_cnt_on_terr,
                   SUM(client_cnt_onl_other) AS client_cnt_onl_other,
                   SUM(client_cnt_onl_rf) AS client_cnt_onl_rf,
                   SUM(client_cnt_other_countries) AS client_cnt_other_countries,
                   SUM(client_cnt_other_region) AS client_cnt_other_region,
                   SUM(client_cnt_other_terr) AS client_cnt_other_terr
            FROM Sber.Upload_csv
            WHERE ISNUMERIC(mcc) != 1
                  AND mcc IS NOT NULL
                  AND [partition_dt] = @actdate
        ) --Сумма по текстовым группам
        BEGIN
            IF EXISTS
            (
                SELECT * --Отбор показателей, чьё отклонение превышает 5%
                FROM
                (
                    SELECT metric1 AS metric,
                           CAST((value1 - value2) / value1 * 100 AS NUMERIC(19, 2)) AS deviation
                    FROM
                    (
                        SELECT metric1,
                               [value1]
                        FROM
                        (
                            SELECT CAST(SUM(trans_amount_on_terr) AS NUMERIC(19, 2)) AS trans_amount_on_terr,
                                   CAST(SUM(trans_amount_onl_other) AS NUMERIC(19, 2)) AS trans_amount_onl_other,
                                   CAST(SUM(trans_amount_onl_rf) AS NUMERIC(19, 2)) AS trans_amount_onl_rf,
                                   CAST(SUM(trans_amount_other_countries) AS NUMERIC(19, 2)) AS trans_amount_other_countries,
                                   CAST(SUM(trans_amount_other_region) AS NUMERIC(19, 2)) AS trans_amount_other_region,
                                   CAST(SUM(trans_amount_other_terr) AS NUMERIC(19, 2)) AS trans_amount_other_terr,
                                   CAST(SUM(count_trans_on_terr) AS NUMERIC(19, 2)) AS count_trans_on_terr,
                                   CAST(SUM(count_trans_onl_other) AS NUMERIC(19, 2)) AS count_trans_onl_other,
                                   CAST(SUM(count_trans_onl_rf) AS NUMERIC(19, 2)) AS count_trans_onl_rf,
                                   CAST(SUM(count_trans_other_countries) AS NUMERIC(19, 2)) AS count_trans_other_countries,
                                   CAST(SUM(count_trans_other_region) AS NUMERIC(19, 2)) AS count_trans_other_region,
                                   CAST(SUM(count_trans_other_terr) AS NUMERIC(19, 2)) AS count_trans_other_terr,
                                   CAST(SUM(client_cnt_on_terr) AS NUMERIC(19, 2)) AS client_cnt_on_terr,
                                   CAST(SUM(client_cnt_onl_other) AS NUMERIC(19, 2)) AS client_cnt_onl_other,
                                   CAST(SUM(client_cnt_onl_rf) AS NUMERIC(19, 2)) AS client_cnt_onl_rf,
                                   CAST(SUM(client_cnt_other_countries) AS NUMERIC(19, 2)) AS client_cnt_other_countries,
                                   CAST(SUM(client_cnt_other_region) AS NUMERIC(19, 2)) AS client_cnt_other_region,
                                   CAST(SUM(client_cnt_other_terr) AS NUMERIC(19, 2)) AS client_cnt_other_terr
                            FROM Sber.Upload_csv
                            WHERE mcc IS NULL
                                  AND [partition_dt] = @actdate
                        ) small1 --Сумма гиперагрегации
                            UNPIVOT([value1] for metric1 in(trans_amount_on_terr, trans_amount_onl_other, trans_amount_onl_rf, trans_amount_other_countries, trans_amount_other_region, trans_amount_other_terr, count_trans_on_terr, count_trans_onl_other, count_trans_onl_rf, count_trans_other_countries, count_trans_other_region, count_trans_other_terr, client_cnt_on_terr, client_cnt_onl_other, client_cnt_onl_rf, client_cnt_other_countries, client_cnt_other_region, client_cnt_other_terr))UNPIV1
                    ) big1
                        INNER JOIN
                        (
                            SELECT *
                            FROM
                            (
                                SELECT metric2,
                                       [value2]
                                FROM
                                (
                                    SELECT CAST(SUM(trans_amount_on_terr) AS NUMERIC(19, 2)) AS trans_amount_on_terr,
                                           CAST(SUM(trans_amount_onl_other) AS NUMERIC(19, 2)) AS trans_amount_onl_other,
                                           CAST(SUM(trans_amount_onl_rf) AS NUMERIC(19, 2)) AS trans_amount_onl_rf,
                                           CAST(SUM(trans_amount_other_countries) AS NUMERIC(19, 2)) AS trans_amount_other_countries,
                                           CAST(SUM(trans_amount_other_region) AS NUMERIC(19, 2)) AS trans_amount_other_region,
                                           CAST(SUM(trans_amount_other_terr) AS NUMERIC(19, 2)) AS trans_amount_other_terr,
                                           CAST(SUM(count_trans_on_terr) AS NUMERIC(19, 2)) AS count_trans_on_terr,
                                           CAST(SUM(count_trans_onl_other) AS NUMERIC(19, 2)) AS count_trans_onl_other,
                                           CAST(SUM(count_trans_onl_rf) AS NUMERIC(19, 2)) AS count_trans_onl_rf,
                                           CAST(SUM(count_trans_other_countries) AS NUMERIC(19, 2)) AS count_trans_other_countries,
                                           CAST(SUM(count_trans_other_region) AS NUMERIC(19, 2)) AS count_trans_other_region,
                                           CAST(SUM(count_trans_other_terr) AS NUMERIC(19, 2)) AS count_trans_other_terr,
                                           CAST(SUM(client_cnt_on_terr) AS NUMERIC(19, 2)) AS client_cnt_on_terr,
                                           CAST(SUM(client_cnt_onl_other) AS NUMERIC(19, 2)) AS client_cnt_onl_other,
                                           CAST(SUM(client_cnt_onl_rf) AS NUMERIC(19, 2)) AS client_cnt_onl_rf,
                                           CAST(SUM(client_cnt_other_countries) AS NUMERIC(19, 2)) AS client_cnt_other_countries,
                                           CAST(SUM(client_cnt_other_region) AS NUMERIC(19, 2)) AS client_cnt_other_region,
                                           CAST(SUM(client_cnt_other_terr) AS NUMERIC(19, 2)) AS client_cnt_other_terr
                                    FROM Sber.Upload_csv
                                    WHERE ISNUMERIC(mcc) != 1
                                          AND mcc IS NOT NULL
                                          AND [partition_dt] = @actdate
                                ) small2 --Сумма по текстовым группам
                                    UNPIVOT([value2] for metric2 in(trans_amount_on_terr, trans_amount_onl_other, trans_amount_onl_rf, trans_amount_other_countries, trans_amount_other_region, trans_amount_other_terr, count_trans_on_terr, count_trans_onl_other, count_trans_onl_rf, count_trans_other_countries, count_trans_other_region, count_trans_other_terr, client_cnt_on_terr, client_cnt_onl_other, client_cnt_onl_rf, client_cnt_other_countries, client_cnt_other_region, client_cnt_other_terr))UNPIV2
                            ) big_
                        ) big2
                            ON big1.metric1 = big2.metric2
                ) large
                WHERE ABS(deviation) >= 5
            ) --Проверка превышения отклонения более, чем на 5%
            BEGIN
                SET @k = @k + 1;
                SELECT * --Вывод таблицы с показателями, чьи отклонениями более 5%
                FROM
                (
                    SELECT metric1 AS 'СУММА ГИПЕРАГРЕГАЦИИ НЕ СОВПАДАЕТ С СУММОЙ ПО ТЕКСТОВЫМ ГРУППАМ В "FL_Trans"',
                           CAST((value1 - value2) / value1 * 100 AS NUMERIC(19, 2)) AS 'Отклонение, %'
                    FROM
                    (
                        SELECT metric1,
                               [value1]
                        FROM
                        (
                            SELECT CAST(SUM(trans_amount_on_terr) AS NUMERIC(19, 2)) AS trans_amount_on_terr,
                                   CAST(SUM(trans_amount_onl_other) AS NUMERIC(19, 2)) AS trans_amount_onl_other,
                                   CAST(SUM(trans_amount_onl_rf) AS NUMERIC(19, 2)) AS trans_amount_onl_rf,
                                   CAST(SUM(trans_amount_other_countries) AS NUMERIC(19, 2)) AS trans_amount_other_countries,
                                   CAST(SUM(trans_amount_other_region) AS NUMERIC(19, 2)) AS trans_amount_other_region,
                                   CAST(SUM(trans_amount_other_terr) AS NUMERIC(19, 2)) AS trans_amount_other_terr,
                                   CAST(SUM(count_trans_on_terr) AS NUMERIC(19, 2)) AS count_trans_on_terr,
                                   CAST(SUM(count_trans_onl_other) AS NUMERIC(19, 2)) AS count_trans_onl_other,
                                   CAST(SUM(count_trans_onl_rf) AS NUMERIC(19, 2)) AS count_trans_onl_rf,
                                   CAST(SUM(count_trans_other_countries) AS NUMERIC(19, 2)) AS count_trans_other_countries,
                                   CAST(SUM(count_trans_other_region) AS NUMERIC(19, 2)) AS count_trans_other_region,
                                   CAST(SUM(count_trans_other_terr) AS NUMERIC(19, 2)) AS count_trans_other_terr,
                                   CAST(SUM(client_cnt_on_terr) AS NUMERIC(19, 2)) AS client_cnt_on_terr,
                                   CAST(SUM(client_cnt_onl_other) AS NUMERIC(19, 2)) AS client_cnt_onl_other,
                                   CAST(SUM(client_cnt_onl_rf) AS NUMERIC(19, 2)) AS client_cnt_onl_rf,
                                   CAST(SUM(client_cnt_other_countries) AS NUMERIC(19, 2)) AS client_cnt_other_countries,
                                   CAST(SUM(client_cnt_other_region) AS NUMERIC(19, 2)) AS client_cnt_other_region,
                                   CAST(SUM(client_cnt_other_terr) AS NUMERIC(19, 2)) AS client_cnt_other_terr
                            FROM Sber.Upload_csv
                            WHERE mcc IS NULL
                                  AND [partition_dt] = @actdate
                        ) small1 --Сумма гипперагрегации
                            UNPIVOT([value1] for metric1 in(trans_amount_on_terr, trans_amount_onl_other, trans_amount_onl_rf, trans_amount_other_countries, trans_amount_other_region, trans_amount_other_terr, count_trans_on_terr, count_trans_onl_other, count_trans_onl_rf, count_trans_other_countries, count_trans_other_region, count_trans_other_terr, client_cnt_on_terr, client_cnt_onl_other, client_cnt_onl_rf, client_cnt_other_countries, client_cnt_other_region, client_cnt_other_terr))UNPIV1
                    ) big1
                        INNER JOIN
                        (
                            SELECT *
                            FROM
                            (
                                SELECT metric2,
                                       [value2]
                                FROM
                                (
                                    SELECT CAST(SUM(trans_amount_on_terr) AS NUMERIC(19, 2)) AS trans_amount_on_terr,
                                           CAST(SUM(trans_amount_onl_other) AS NUMERIC(19, 2)) AS trans_amount_onl_other,
                                           CAST(SUM(trans_amount_onl_rf) AS NUMERIC(19, 2)) AS trans_amount_onl_rf,
                                           CAST(SUM(trans_amount_other_countries) AS NUMERIC(19, 2)) AS trans_amount_other_countries,
                                           CAST(SUM(trans_amount_other_region) AS NUMERIC(19, 2)) AS trans_amount_other_region,
                                           CAST(SUM(trans_amount_other_terr) AS NUMERIC(19, 2)) AS trans_amount_other_terr,
                                           CAST(SUM(count_trans_on_terr) AS NUMERIC(19, 2)) AS count_trans_on_terr,
                                           CAST(SUM(count_trans_onl_other) AS NUMERIC(19, 2)) AS count_trans_onl_other,
                                           CAST(SUM(count_trans_onl_rf) AS NUMERIC(19, 2)) AS count_trans_onl_rf,
                                           CAST(SUM(count_trans_other_countries) AS NUMERIC(19, 2)) AS count_trans_other_countries,
                                           CAST(SUM(count_trans_other_region) AS NUMERIC(19, 2)) AS count_trans_other_region,
                                           CAST(SUM(count_trans_other_terr) AS NUMERIC(19, 2)) AS count_trans_other_terr,
                                           CAST(SUM(client_cnt_on_terr) AS NUMERIC(19, 2)) AS client_cnt_on_terr,
                                           CAST(SUM(client_cnt_onl_other) AS NUMERIC(19, 2)) AS client_cnt_onl_other,
                                           CAST(SUM(client_cnt_onl_rf) AS NUMERIC(19, 2)) AS client_cnt_onl_rf,
                                           CAST(SUM(client_cnt_other_countries) AS NUMERIC(19, 2)) AS client_cnt_other_countries,
                                           CAST(SUM(client_cnt_other_region) AS NUMERIC(19, 2)) AS client_cnt_other_region,
                                           CAST(SUM(client_cnt_other_terr) AS NUMERIC(19, 2)) AS client_cnt_other_terr
                                    FROM Sber.Upload_csv
                                    WHERE ISNUMERIC(mcc) != 1
                                          AND mcc IS NOT NULL
                                          AND [partition_dt] = @actdate
                                ) small2 --Сумма по текстовым группам
                                    UNPIVOT([value2] for metric2 in(trans_amount_on_terr, trans_amount_onl_other, trans_amount_onl_rf, trans_amount_other_countries, trans_amount_other_region, trans_amount_other_terr, count_trans_on_terr, count_trans_onl_other, count_trans_onl_rf, count_trans_other_countries, count_trans_other_region, count_trans_other_terr, client_cnt_on_terr, client_cnt_onl_other, client_cnt_onl_rf, client_cnt_other_countries, client_cnt_other_region, client_cnt_other_terr))UNPIV2
                            ) big_
                        ) big2
                            ON big1.metric1 = big2.metric2
                ) large
                WHERE ABS([Отклонение, %]) >= 5;
            END
        END
        IF EXISTS
        (
            SELECT SUM(trans_amount_on_terr) AS trans_amount_on_terr,
                   SUM(trans_amount_onl_other) AS trans_amount_onl_other, --Гиперагрегация | Числовые коды
                   SUM(trans_amount_onl_rf) AS trans_amount_onl_rf,
                   SUM(trans_amount_other_countries) AS trans_amount_other_countries,
                   SUM(trans_amount_other_region) AS trans_amount_other_region,
                   SUM(trans_amount_other_terr) AS trans_amount_other_terr,
                   SUM(count_trans_on_terr) AS count_trans_on_terr,
                   SUM(count_trans_onl_other) AS count_trans_onl_other,
                   SUM(count_trans_onl_rf) AS count_trans_onl_rf,
                   SUM(count_trans_other_countries) AS count_trans_other_countries,
                   SUM(count_trans_other_region) AS count_trans_other_region,
                   SUM(count_trans_other_terr) AS count_trans_other_terr,
                   SUM(client_cnt_on_terr) AS client_cnt_on_terr,
                   SUM(client_cnt_onl_other) AS client_cnt_onl_other,
                   SUM(client_cnt_onl_rf) AS client_cnt_onl_rf,
                   SUM(client_cnt_other_countries) AS client_cnt_other_countries,
                   SUM(client_cnt_other_region) AS client_cnt_other_region,
                   SUM(client_cnt_other_terr) AS client_cnt_other_terr
            FROM Sber.Upload_csv
            WHERE mcc IS NULL
                  AND [partition_dt] = @actdate --Сумма гиперагрегации
            EXCEPT
            SELECT SUM(trans_amount_on_terr) AS trans_amount_on_terr,
                   SUM(trans_amount_onl_other) AS trans_amount_onl_other,
                   SUM(trans_amount_onl_rf) AS trans_amount_onl_rf,
                   SUM(trans_amount_other_countries) AS trans_amount_other_countries,
                   SUM(trans_amount_other_region) AS trans_amount_other_region,
                   SUM(trans_amount_other_terr) AS trans_amount_other_terr,
                   SUM(count_trans_on_terr) AS count_trans_on_terr,
                   SUM(count_trans_onl_other) AS count_trans_onl_other,
                   SUM(count_trans_onl_rf) AS count_trans_onl_rf,
                   SUM(count_trans_other_countries) AS count_trans_other_countries,
                   SUM(count_trans_other_region) AS count_trans_other_region,
                   SUM(count_trans_other_terr) AS count_trans_other_terr,
                   SUM(client_cnt_on_terr) AS client_cnt_on_terr,
                   SUM(client_cnt_onl_other) AS client_cnt_onl_other,
                   SUM(client_cnt_onl_rf) AS client_cnt_onl_rf,
                   SUM(client_cnt_other_countries) AS client_cnt_other_countries,
                   SUM(client_cnt_other_region) AS client_cnt_other_region,
                   SUM(client_cnt_other_terr) AS client_cnt_other_terr
            FROM Sber.Upload_csv
            WHERE ISNUMERIC(mcc) = 1
                  AND [partition_dt] = @actdate
        ) --Сумма по числовым кодам
        BEGIN
            IF EXISTS
            (
                SELECT * --Отбор показателей, чьё отклонение превышает 5%
                FROM
                (
                    SELECT metric1 AS metric,
                           CAST((value1 - value2) / value1 * 100 AS NUMERIC(19, 2)) AS deviation
                    FROM
                    (
                        SELECT metric1,
                               [value1]
                        FROM
                        (
                            SELECT CAST(SUM(trans_amount_on_terr) AS NUMERIC(19, 2)) AS trans_amount_on_terr,
                                   CAST(SUM(trans_amount_onl_other) AS NUMERIC(19, 2)) AS trans_amount_onl_other,
                                   CAST(SUM(trans_amount_onl_rf) AS NUMERIC(19, 2)) AS trans_amount_onl_rf,
                                   CAST(SUM(trans_amount_other_countries) AS NUMERIC(19, 2)) AS trans_amount_other_countries,
                                   CAST(SUM(trans_amount_other_region) AS NUMERIC(19, 2)) AS trans_amount_other_region,
                                   CAST(SUM(trans_amount_other_terr) AS NUMERIC(19, 2)) AS trans_amount_other_terr,
                                   CAST(SUM(count_trans_on_terr) AS NUMERIC(19, 2)) AS count_trans_on_terr,
                                   CAST(SUM(count_trans_onl_other) AS NUMERIC(19, 2)) AS count_trans_onl_other,
                                   CAST(SUM(count_trans_onl_rf) AS NUMERIC(19, 2)) AS count_trans_onl_rf,
                                   CAST(SUM(count_trans_other_countries) AS NUMERIC(19, 2)) AS count_trans_other_countries,
                                   CAST(SUM(count_trans_other_region) AS NUMERIC(19, 2)) AS count_trans_other_region,
                                   CAST(SUM(count_trans_other_terr) AS NUMERIC(19, 2)) AS count_trans_other_terr,
                                   CAST(SUM(client_cnt_on_terr) AS NUMERIC(19, 2)) AS client_cnt_on_terr,
                                   CAST(SUM(client_cnt_onl_other) AS NUMERIC(19, 2)) AS client_cnt_onl_other,
                                   CAST(SUM(client_cnt_onl_rf) AS NUMERIC(19, 2)) AS client_cnt_onl_rf,
                                   CAST(SUM(client_cnt_other_countries) AS NUMERIC(19, 2)) AS client_cnt_other_countries,
                                   CAST(SUM(client_cnt_other_region) AS NUMERIC(19, 2)) AS client_cnt_other_region,
                                   CAST(SUM(client_cnt_other_terr) AS NUMERIC(19, 2)) AS client_cnt_other_terr
                            FROM Sber.Upload_csv
                            WHERE mcc IS NULL
                                  AND [partition_dt] = @actdate
                        ) small1 --Сумма гиперагрегации
                            UNPIVOT([value1] for metric1 in(trans_amount_on_terr, trans_amount_onl_other, trans_amount_onl_rf, trans_amount_other_countries, trans_amount_other_region, trans_amount_other_terr, count_trans_on_terr, count_trans_onl_other, count_trans_onl_rf, count_trans_other_countries, count_trans_other_region, count_trans_other_terr, client_cnt_on_terr, client_cnt_onl_other, client_cnt_onl_rf, client_cnt_other_countries, client_cnt_other_region, client_cnt_other_terr))UNPIV1
                    ) big1
                        INNER JOIN
                        (
                            SELECT *
                            FROM
                            (
                                SELECT metric2,
                                       [value2]
                                FROM
                                (
                                    SELECT CAST(SUM(trans_amount_on_terr) AS NUMERIC(19, 2)) AS trans_amount_on_terr,
                                           CAST(SUM(trans_amount_onl_other) AS NUMERIC(19, 2)) AS trans_amount_onl_other,
                                           CAST(SUM(trans_amount_onl_rf) AS NUMERIC(19, 2)) AS trans_amount_onl_rf,
                                           CAST(SUM(trans_amount_other_countries) AS NUMERIC(19, 2)) AS trans_amount_other_countries,
                                           CAST(SUM(trans_amount_other_region) AS NUMERIC(19, 2)) AS trans_amount_other_region,
                                           CAST(SUM(trans_amount_other_terr) AS NUMERIC(19, 2)) AS trans_amount_other_terr,
                                           CAST(SUM(count_trans_on_terr) AS NUMERIC(19, 2)) AS count_trans_on_terr,
                                           CAST(SUM(count_trans_onl_other) AS NUMERIC(19, 2)) AS count_trans_onl_other,
                                           CAST(SUM(count_trans_onl_rf) AS NUMERIC(19, 2)) AS count_trans_onl_rf,
                                           CAST(SUM(count_trans_other_countries) AS NUMERIC(19, 2)) AS count_trans_other_countries,
                                           CAST(SUM(count_trans_other_region) AS NUMERIC(19, 2)) AS count_trans_other_region,
                                           CAST(SUM(count_trans_other_terr) AS NUMERIC(19, 2)) AS count_trans_other_terr,
                                           CAST(SUM(client_cnt_on_terr) AS NUMERIC(19, 2)) AS client_cnt_on_terr,
                                           CAST(SUM(client_cnt_onl_other) AS NUMERIC(19, 2)) AS client_cnt_onl_other,
                                           CAST(SUM(client_cnt_onl_rf) AS NUMERIC(19, 2)) AS client_cnt_onl_rf,
                                           CAST(SUM(client_cnt_other_countries) AS NUMERIC(19, 2)) AS client_cnt_other_countries,
                                           CAST(SUM(client_cnt_other_region) AS NUMERIC(19, 2)) AS client_cnt_other_region,
                                           CAST(SUM(client_cnt_other_terr) AS NUMERIC(19, 2)) AS client_cnt_other_terr
                                    FROM Sber.Upload_csv
                                    WHERE ISNUMERIC(mcc) = 1
                                          AND [partition_dt] = @actdate
                                ) small2 --Сумма по числовым кодам
                                    UNPIVOT([value2] for metric2 in(trans_amount_on_terr, trans_amount_onl_other, trans_amount_onl_rf, trans_amount_other_countries, trans_amount_other_region, trans_amount_other_terr, count_trans_on_terr, count_trans_onl_other, count_trans_onl_rf, count_trans_other_countries, count_trans_other_region, count_trans_other_terr, client_cnt_on_terr, client_cnt_onl_other, client_cnt_onl_rf, client_cnt_other_countries, client_cnt_other_region, client_cnt_other_terr))UNPIV2
                            ) big_
                        ) big2
                            ON big1.metric1 = big2.metric2
                ) large
                WHERE ABS(deviation) >= 5
            ) --Проверка превышения отклонения более, чем на 5%
            BEGIN
                SET @k = @k + 1;
                SELECT * --Вывод таблицы с показателями, чьи отклонениями более 5%
                FROM
                (
                    SELECT metric1 AS 'СУММА ГИПЕРАГРЕГАЦИИ НЕ СОВПАДАЕТ С СУММОЙ ПО ЧИСЛОВЫМ КОДАМ В "FL_Trans"',
                           CAST((value1 - value2) / value1 * 100 AS NUMERIC(19, 2)) AS 'Отклонение, %'
                    FROM
                    (
                        SELECT metric1,
                               [value1]
                        FROM
                        (
                            SELECT CAST(SUM(trans_amount_on_terr) AS NUMERIC(19, 2)) AS trans_amount_on_terr,
                                   CAST(SUM(trans_amount_onl_other) AS NUMERIC(19, 2)) AS trans_amount_onl_other,
                                   CAST(SUM(trans_amount_onl_rf) AS NUMERIC(19, 2)) AS trans_amount_onl_rf,
                                   CAST(SUM(trans_amount_other_countries) AS NUMERIC(19, 2)) AS trans_amount_other_countries,
                                   CAST(SUM(trans_amount_other_region) AS NUMERIC(19, 2)) AS trans_amount_other_region,
                                   CAST(SUM(trans_amount_other_terr) AS NUMERIC(19, 2)) AS trans_amount_other_terr,
                                   CAST(SUM(count_trans_on_terr) AS NUMERIC(19, 2)) AS count_trans_on_terr,
                                   CAST(SUM(count_trans_onl_other) AS NUMERIC(19, 2)) AS count_trans_onl_other,
                                   CAST(SUM(count_trans_onl_rf) AS NUMERIC(19, 2)) AS count_trans_onl_rf,
                                   CAST(SUM(count_trans_other_countries) AS NUMERIC(19, 2)) AS count_trans_other_countries,
                                   CAST(SUM(count_trans_other_region) AS NUMERIC(19, 2)) AS count_trans_other_region,
                                   CAST(SUM(count_trans_other_terr) AS NUMERIC(19, 2)) AS count_trans_other_terr,
                                   CAST(SUM(client_cnt_on_terr) AS NUMERIC(19, 2)) AS client_cnt_on_terr,
                                   CAST(SUM(client_cnt_onl_other) AS NUMERIC(19, 2)) AS client_cnt_onl_other,
                                   CAST(SUM(client_cnt_onl_rf) AS NUMERIC(19, 2)) AS client_cnt_onl_rf,
                                   CAST(SUM(client_cnt_other_countries) AS NUMERIC(19, 2)) AS client_cnt_other_countries,
                                   CAST(SUM(client_cnt_other_region) AS NUMERIC(19, 2)) AS client_cnt_other_region,
                                   CAST(SUM(client_cnt_other_terr) AS NUMERIC(19, 2)) AS client_cnt_other_terr
                            FROM Sber.Upload_csv
                            WHERE mcc IS NULL
                                  AND [partition_dt] = @actdate
                        ) small1 --Сумма гипперагрегации
                            UNPIVOT([value1] for metric1 in(trans_amount_on_terr, trans_amount_onl_other, trans_amount_onl_rf, trans_amount_other_countries, trans_amount_other_region, trans_amount_other_terr, count_trans_on_terr, count_trans_onl_other, count_trans_onl_rf, count_trans_other_countries, count_trans_other_region, count_trans_other_terr, client_cnt_on_terr, client_cnt_onl_other, client_cnt_onl_rf, client_cnt_other_countries, client_cnt_other_region, client_cnt_other_terr))UNPIV1
                    ) big1
                        INNER JOIN
                        (
                            SELECT *
                            FROM
                            (
                                SELECT metric2,
                                       [value2]
                                FROM
                                (
                                    SELECT CAST(SUM(trans_amount_on_terr) AS NUMERIC(19, 2)) AS trans_amount_on_terr,
                                           CAST(SUM(trans_amount_onl_other) AS NUMERIC(19, 2)) AS trans_amount_onl_other,
                                           CAST(SUM(trans_amount_onl_rf) AS NUMERIC(19, 2)) AS trans_amount_onl_rf,
                                           CAST(SUM(trans_amount_other_countries) AS NUMERIC(19, 2)) AS trans_amount_other_countries,
                                           CAST(SUM(trans_amount_other_region) AS NUMERIC(19, 2)) AS trans_amount_other_region,
                                           CAST(SUM(trans_amount_other_terr) AS NUMERIC(19, 2)) AS trans_amount_other_terr,
                                           CAST(SUM(count_trans_on_terr) AS NUMERIC(19, 2)) AS count_trans_on_terr,
                                           CAST(SUM(count_trans_onl_other) AS NUMERIC(19, 2)) AS count_trans_onl_other,
                                           CAST(SUM(count_trans_onl_rf) AS NUMERIC(19, 2)) AS count_trans_onl_rf,
                                           CAST(SUM(count_trans_other_countries) AS NUMERIC(19, 2)) AS count_trans_other_countries,
                                           CAST(SUM(count_trans_other_region) AS NUMERIC(19, 2)) AS count_trans_other_region,
                                           CAST(SUM(count_trans_other_terr) AS NUMERIC(19, 2)) AS count_trans_other_terr,
                                           CAST(SUM(client_cnt_on_terr) AS NUMERIC(19, 2)) AS client_cnt_on_terr,
                                           CAST(SUM(client_cnt_onl_other) AS NUMERIC(19, 2)) AS client_cnt_onl_other,
                                           CAST(SUM(client_cnt_onl_rf) AS NUMERIC(19, 2)) AS client_cnt_onl_rf,
                                           CAST(SUM(client_cnt_other_countries) AS NUMERIC(19, 2)) AS client_cnt_other_countries,
                                           CAST(SUM(client_cnt_other_region) AS NUMERIC(19, 2)) AS client_cnt_other_region,
                                           CAST(SUM(client_cnt_other_terr) AS NUMERIC(19, 2)) AS client_cnt_other_terr
                                    FROM Sber.Upload_csv
                                    WHERE ISNUMERIC(mcc) = 1
                                          AND [partition_dt] = @actdate
                                ) small2 --Сумма по числовым кодам
                                    UNPIVOT([value2] for metric2 in(trans_amount_on_terr, trans_amount_onl_other, trans_amount_onl_rf, trans_amount_other_countries, trans_amount_other_region, trans_amount_other_terr, count_trans_on_terr, count_trans_onl_other, count_trans_onl_rf, count_trans_other_countries, count_trans_other_region, count_trans_other_terr, client_cnt_on_terr, client_cnt_onl_other, client_cnt_onl_rf, client_cnt_other_countries, client_cnt_other_region, client_cnt_other_terr))UNPIV2
                            ) big_
                        ) big2
                            ON big1.metric1 = big2.metric2
                ) large
                WHERE ABS([Отклонение, %]) >= 5;
            END
        END
        IF EXISTS
        (
            SELECT * --Проверка равенства сумм показателей текстовых групп и числовых кодов, распределённых согласно таблице "MccGroup"
            FROM
            (
                SELECT TOP 1000
                    mcc,
                    region_name,
                    SUM(trans_amount_on_terr) AS trans_amount_on_terr,
                    SUM(trans_amount_onl_other) AS trans_amount_onl_other,
                    SUM(trans_amount_onl_rf) AS trans_amount_onl_rf,
                    SUM(trans_amount_other_countries) AS trans_amount_other_countries,
                    SUM(trans_amount_other_region) AS trans_amount_other_region,
                    SUM(trans_amount_other_terr) AS trans_amount_other_terr,
                    SUM(count_trans_on_terr) AS count_trans_on_terr,
                    SUM(count_trans_onl_other) AS count_trans_onl_other,
                    SUM(count_trans_onl_rf) AS count_trans_onl_rf,
                    SUM(count_trans_other_countries) AS count_trans_other_countries,
                    SUM(count_trans_other_region) AS count_trans_other_region,
                    SUM(count_trans_other_terr) AS count_trans_other_terr,
                    SUM(client_cnt_on_terr) AS client_cnt_on_terr,
                    SUM(client_cnt_onl_other) AS client_cnt_onl_other,
                    SUM(client_cnt_onl_rf) AS client_cnt_onl_rf,
                    SUM(client_cnt_other_countries) AS client_cnt_other_countries,
                    SUM(client_cnt_other_region) AS client_cnt_other_region,
                    SUM(client_cnt_other_terr) AS client_cnt_other_terr,
                    partition_dt
                FROM Sber.Upload_csv
                WHERE ISNUMERIC(mcc) != 1
                      AND partition_dt = @actdate
                      AND mcc IS NOT NULL --Текстовые группы
                GROUP BY mcc,
                         region_name,
                         partition_dt
                ORDER BY mcc
            ) x
            EXCEPT
            SELECT *
            FROM
            (
                SELECT TOP 1000
                    mcc_group,
                    region_name,
                    SUM(trans_amount_on_terr) AS trans_amount_on_terr,
                    SUM(trans_amount_onl_other) AS trans_amount_onl_other,
                    SUM(trans_amount_onl_rf) AS trans_amount_onl_rf,
                    SUM(trans_amount_other_countries) AS trans_amount_other_countries,
                    SUM(trans_amount_other_region) AS trans_amount_other_region,
                    SUM(trans_amount_other_terr) AS trans_amount_other_terr,
                    SUM(count_trans_on_terr) AS count_trans_on_terr,
                    SUM(count_trans_onl_other) AS count_trans_onl_other,
                    SUM(count_trans_onl_rf) AS count_trans_onl_rf,
                    SUM(count_trans_other_countries) AS count_trans_other_countries,
                    SUM(count_trans_other_region) AS count_trans_other_region,
                    SUM(count_trans_other_terr) AS count_trans_other_terr,
                    SUM(client_cnt_on_terr) AS client_cnt_on_terr,
                    SUM(client_cnt_onl_other) AS client_cnt_onl_other,
                    SUM(client_cnt_onl_rf) AS client_cnt_onl_rf,
                    SUM(client_cnt_other_countries) AS client_cnt_other_countries,
                    SUM(client_cnt_other_region) AS client_cnt_other_region,
                    SUM(client_cnt_other_terr) AS client_cnt_other_terr,
                    partition_dt
                FROM
                (
                    SELECT *
                    FROM
                    (
                        SELECT mcc,
                               region_name,
                               SUM(trans_amount_on_terr) AS trans_amount_on_terr,
                               SUM(trans_amount_onl_other) AS trans_amount_onl_other,
                               SUM(trans_amount_onl_rf) AS trans_amount_onl_rf,
                               SUM(trans_amount_other_countries) AS trans_amount_other_countries,
                               SUM(trans_amount_other_region) AS trans_amount_other_region,
                               SUM(trans_amount_other_terr) AS trans_amount_other_terr,
                               SUM(count_trans_on_terr) AS count_trans_on_terr,
                               SUM(count_trans_onl_other) AS count_trans_onl_other,
                               SUM(count_trans_onl_rf) AS count_trans_onl_rf,
                               SUM(count_trans_other_countries) AS count_trans_other_countries,
                               SUM(count_trans_other_region) AS count_trans_other_region,
                               SUM(count_trans_other_terr) AS count_trans_other_terr,
                               SUM(client_cnt_on_terr) AS client_cnt_on_terr,
                               SUM(client_cnt_onl_other) AS client_cnt_onl_other,
                               SUM(client_cnt_onl_rf) AS client_cnt_onl_rf,
                               SUM(client_cnt_other_countries) AS client_cnt_other_countries,
                               SUM(client_cnt_other_region) AS client_cnt_other_region,
                               SUM(client_cnt_other_terr) AS client_cnt_other_terr,
                               partition_dt
                        FROM Sber.Upload_csv
                        WHERE ISNUMERIC(mcc) = 1
                              AND partition_dt = @actdate --Числовые коды
                        GROUP BY mcc,
                                 region_name,
                                 partition_dt
                    ) t1
                        LEFT JOIN Sber.MccGroup t2
                            ON t1.mcc = t2.mcc_code
                ) big
                GROUP BY mcc_group,
                         region_name,
                         partition_dt
                ORDER BY mcc_group
            ) y
        )
        BEGIN
            IF EXISTS
            (
                SELECT large1.mcc,
                       large1.region_name, --Расчёт отклонений в %
                       CAST((large1.trans_amount_on_terr - large2.trans_amount_on_terr)
                            / NULLIF(large1.trans_amount_on_terr, 0) * 100 AS NUMERIC(19, 2)) AS 'trans_amount_on_terr (deviation in %)',
                       CAST((large1.trans_amount_onl_other - large2.trans_amount_onl_other)
                            / NULLIF(large1.trans_amount_onl_other, 0) * 100 AS NUMERIC(19, 2)) AS 'trans_amount_onl_other_deviation (deviation in %)',
                       CAST((large1.trans_amount_onl_rf - large2.trans_amount_onl_rf)
                            / NULLIF(large1.trans_amount_onl_rf, 0) * 100 AS NUMERIC(19, 2)) AS 'trans_amount_onl_rf (deviation in %)',
                       CAST((large1.trans_amount_other_countries - large2.trans_amount_other_countries)
                            / NULLIF(large1.trans_amount_other_countries, 0) * 100 AS NUMERIC(19, 2)) AS 'trans_amount_other_countries(deviation in %)',
                       CAST((large1.trans_amount_other_region - large2.trans_amount_other_countries)
                            / NULLIF(large1.trans_amount_other_region, 0) * 100 AS NUMERIC(19, 2)) AS 'trans_amount_other_region (deviation in %)',
                       CAST((large1.trans_amount_other_terr - large2.trans_amount_other_countries)
                            / NULLIF(large1.trans_amount_other_terr, 0) * 100 AS NUMERIC(19, 2)) AS 'trans_amount_other_terr (deviation in %)',
                       CAST((large1.count_trans_on_terr - large2.count_trans_on_terr)
                            / NULLIF(large1.count_trans_on_terr, 0) * 100 AS NUMERIC(19, 2)) AS 'count_trans_on_terr (deviation in %)',
                       CAST((large1.count_trans_onl_other - large2.count_trans_onl_other)
                            / NULLIF(large1.count_trans_onl_other, 0) * 100 AS NUMERIC(19, 2)) AS 'count_trans_onl_other (deviation in %)',
                       CAST((large1.count_trans_onl_rf - large2.count_trans_onl_rf)
                            / NULLIF(large1.count_trans_onl_rf, 0) * 100 AS NUMERIC(19, 2)) AS 'count_trans_onl_rf (deviation in %)',
                       CAST((large1.count_trans_other_countries - large2.count_trans_other_countries)
                            / NULLIF(large1.count_trans_other_countries, 0) * 100 AS NUMERIC(19, 2)) AS 'count_trans_other_countries (deviation in %)',
                       CAST((large1.count_trans_other_region - large2.count_trans_other_region)
                            / NULLIF(large1.count_trans_other_region, 0) * 100 AS NUMERIC(19, 2)) AS 'count_trans_other_region (deviation in %)',
                       CAST((large1.count_trans_other_terr - large2.count_trans_other_terr)
                            / NULLIF(large1.count_trans_other_terr, 0) * 100 AS NUMERIC(19, 2)) AS 'count_trans_other_terr (deviation in %)',
                       CAST((large1.client_cnt_on_terr - large2.client_cnt_on_terr)
                            / NULLIF(large1.client_cnt_on_terr, 0) * 100 AS NUMERIC(19, 2)) AS 'client_cnt_on_terr (deviation in %)',
                       CAST((large1.client_cnt_onl_other - large2.client_cnt_onl_other)
                            / NULLIF(large1.client_cnt_onl_other, 0) * 100 AS NUMERIC(19, 2)) AS 'client_cnt_onl_other (deviation in %)',
                       CAST((large1.client_cnt_onl_rf - large2.client_cnt_onl_rf) / NULLIF(large1.client_cnt_onl_rf, 0)
                            * 100 AS NUMERIC(19, 2)) AS 'client_cnt_onl_rf (deviation in %)',
                       CAST((large1.client_cnt_other_countries - large2.client_cnt_other_countries)
                            / NULLIF(large1.client_cnt_other_countries, 0) * 100 AS NUMERIC(19, 2)) AS 'client_cnt_other_countries (deviation in %)',
                       CAST((large1.client_cnt_other_region - large2.client_cnt_other_region)
                            / NULLIF(large1.client_cnt_other_region, 0) * 100 AS NUMERIC(19, 2)) AS 'client_cnt_other_region (deviation in %)',
                       CAST((large1.client_cnt_other_terr - large2.client_cnt_other_terr)
                            / NULLIF(large1.client_cnt_other_terr, 0) * 100 AS NUMERIC(19, 2)) AS 'client_cnt_other_terr (deviation in %)',
                       large1.partition_dt
                FROM
                (
                    SELECT * --Расчёт отклонений показателей у неравных текстовых групп
                    FROM
                    (
                        SELECT TOP 1000
                            mcc,
                            region_name,
                            SUM(trans_amount_on_terr) AS trans_amount_on_terr,
                            SUM(trans_amount_onl_other) AS trans_amount_onl_other,
                            SUM(trans_amount_onl_rf) AS trans_amount_onl_rf,
                            SUM(trans_amount_other_countries) AS trans_amount_other_countries,
                            SUM(trans_amount_other_region) AS trans_amount_other_region,
                            SUM(trans_amount_other_terr) AS trans_amount_other_terr,
                            SUM(count_trans_on_terr) AS count_trans_on_terr,
                            SUM(count_trans_onl_other) AS count_trans_onl_other,
                            SUM(count_trans_onl_rf) AS count_trans_onl_rf,
                            SUM(count_trans_other_countries) AS count_trans_other_countries,
                            SUM(count_trans_other_region) AS count_trans_other_region,
                            SUM(count_trans_other_terr) AS count_trans_other_terr,
                            SUM(client_cnt_on_terr) AS client_cnt_on_terr,
                            SUM(client_cnt_onl_other) AS client_cnt_onl_other,
                            SUM(client_cnt_onl_rf) AS client_cnt_onl_rf,
                            SUM(client_cnt_other_countries) AS client_cnt_other_countries,
                            SUM(client_cnt_other_region) AS client_cnt_other_region,
                            SUM(client_cnt_other_terr) AS client_cnt_other_terr,
                            partition_dt
                        FROM Sber.Upload_csv
                        WHERE ISNUMERIC(mcc) != 1
                              AND partition_dt = @actdate
                              AND mcc IS NOT NULL --Текстовые группы
                        GROUP BY mcc,
                                 region_name,
                                 partition_dt
                        ORDER BY mcc
                    ) x
                    EXCEPT
                    SELECT *
                    FROM
                    (
                        SELECT TOP 1000
                            mcc_group,
                            region_name,
                            SUM(trans_amount_on_terr) AS trans_amount_on_terr,
                            SUM(trans_amount_onl_other) AS trans_amount_onl_other,
                            SUM(trans_amount_onl_rf) AS trans_amount_onl_rf,
                            SUM(trans_amount_other_countries) AS trans_amount_other_countries,
                            SUM(trans_amount_other_region) AS trans_amount_other_region,
                            SUM(trans_amount_other_terr) AS trans_amount_other_terr,
                            SUM(count_trans_on_terr) AS count_trans_on_terr,
                            SUM(count_trans_onl_other) AS count_trans_onl_other,
                            SUM(count_trans_onl_rf) AS count_trans_onl_rf,
                            SUM(count_trans_other_countries) AS count_trans_other_countries,
                            SUM(count_trans_other_region) AS count_trans_other_region,
                            SUM(count_trans_other_terr) AS count_trans_other_terr,
                            SUM(client_cnt_on_terr) AS client_cnt_on_terr,
                            SUM(client_cnt_onl_other) AS client_cnt_onl_other,
                            SUM(client_cnt_onl_rf) AS client_cnt_onl_rf,
                            SUM(client_cnt_other_countries) AS client_cnt_other_countries,
                            SUM(client_cnt_other_region) AS client_cnt_other_region,
                            SUM(client_cnt_other_terr) AS client_cnt_other_terr,
                            partition_dt
                        FROM
                        (
                            SELECT *
                            FROM
                            (
                                SELECT mcc,
                                       region_name,
                                       SUM(trans_amount_on_terr) AS trans_amount_on_terr,
                                       SUM(trans_amount_onl_other) AS trans_amount_onl_other,
                                       SUM(trans_amount_onl_rf) AS trans_amount_onl_rf,
                                       SUM(trans_amount_other_countries) AS trans_amount_other_countries,
                                       SUM(trans_amount_other_region) AS trans_amount_other_region,
                                       SUM(trans_amount_other_terr) AS trans_amount_other_terr,
                                       SUM(count_trans_on_terr) AS count_trans_on_terr,
                                       SUM(count_trans_onl_other) AS count_trans_onl_other,
                                       SUM(count_trans_onl_rf) AS count_trans_onl_rf,
                                       SUM(count_trans_other_countries) AS count_trans_other_countries,
                                       SUM(count_trans_other_region) AS count_trans_other_region,
                                       SUM(count_trans_other_terr) AS count_trans_other_terr,
                                       SUM(client_cnt_on_terr) AS client_cnt_on_terr,
                                       SUM(client_cnt_onl_other) AS client_cnt_onl_other,
                                       SUM(client_cnt_onl_rf) AS client_cnt_onl_rf,
                                       SUM(client_cnt_other_countries) AS client_cnt_other_countries,
                                       SUM(client_cnt_other_region) AS client_cnt_other_region,
                                       SUM(client_cnt_other_terr) AS client_cnt_other_terr,
                                       partition_dt
                                FROM Sber.Upload_csv
                                WHERE ISNUMERIC(mcc) = 1
                                      AND partition_dt = @actdate --Числовые коды
                                GROUP BY mcc,
                                         region_name,
                                         partition_dt
                            ) t1
                                LEFT JOIN Sber.MccGroup t2
                                    ON t1.mcc = t2.mcc_code
                        ) big
                        GROUP BY mcc_group,
                                 region_name,
                                 partition_dt
                        ORDER BY mcc_group
                    ) y
                ) large1
                    LEFT JOIN
                    (
                        SELECT *
                        FROM
                        (
                            SELECT TOP 1000
                                mcc_group,
                                region_name,
                                SUM(trans_amount_on_terr) AS trans_amount_on_terr,
                                SUM(trans_amount_onl_other) AS trans_amount_onl_other,
                                SUM(trans_amount_onl_rf) AS trans_amount_onl_rf,
                                SUM(trans_amount_other_countries) AS trans_amount_other_countries,
                                SUM(trans_amount_other_region) AS trans_amount_other_region,
                                SUM(trans_amount_other_terr) AS trans_amount_other_terr,
                                SUM(count_trans_on_terr) AS count_trans_on_terr,
                                SUM(count_trans_onl_other) AS count_trans_onl_other,
                                SUM(count_trans_onl_rf) AS count_trans_onl_rf,
                                SUM(count_trans_other_countries) AS count_trans_other_countries,
                                SUM(count_trans_other_region) AS count_trans_other_region,
                                SUM(count_trans_other_terr) AS count_trans_other_terr,
                                SUM(client_cnt_on_terr) AS client_cnt_on_terr,
                                SUM(client_cnt_onl_other) AS client_cnt_onl_other,
                                SUM(client_cnt_onl_rf) AS client_cnt_onl_rf,
                                SUM(client_cnt_other_countries) AS client_cnt_other_countries,
                                SUM(client_cnt_other_region) AS client_cnt_other_region,
                                SUM(client_cnt_other_terr) AS client_cnt_other_terr,
                                partition_dt
                            FROM
                            (
                                SELECT *
                                FROM
                                (
                                    SELECT mcc,
                                           region_name,
                                           SUM(trans_amount_on_terr) AS trans_amount_on_terr,
                                           SUM(trans_amount_onl_other) AS trans_amount_onl_other,
                                           SUM(trans_amount_onl_rf) AS trans_amount_onl_rf,
                                           SUM(trans_amount_other_countries) AS trans_amount_other_countries,
                                           SUM(trans_amount_other_region) AS trans_amount_other_region,
                                           SUM(trans_amount_other_terr) AS trans_amount_other_terr,
                                           SUM(count_trans_on_terr) AS count_trans_on_terr,
                                           SUM(count_trans_onl_other) AS count_trans_onl_other,
                                           SUM(count_trans_onl_rf) AS count_trans_onl_rf,
                                           SUM(count_trans_other_countries) AS count_trans_other_countries,
                                           SUM(count_trans_other_region) AS count_trans_other_region,
                                           SUM(count_trans_other_terr) AS count_trans_other_terr,
                                           SUM(client_cnt_on_terr) AS client_cnt_on_terr,
                                           SUM(client_cnt_onl_other) AS client_cnt_onl_other,
                                           SUM(client_cnt_onl_rf) AS client_cnt_onl_rf,
                                           SUM(client_cnt_other_countries) AS client_cnt_other_countries,
                                           SUM(client_cnt_other_region) AS client_cnt_other_region,
                                           SUM(client_cnt_other_terr) AS client_cnt_other_terr,
                                           partition_dt
                                    FROM Sber.Upload_csv
                                    WHERE ISNUMERIC(mcc) = 1
                                          AND partition_dt = @actdate --Числовые коды
                                    GROUP BY mcc,
                                             region_name,
                                             partition_dt
                                ) t1
                                    LEFT JOIN Sber.MccGroup t2
                                        ON t1.mcc = t2.mcc_code
                            ) big
                            GROUP BY mcc_group,
                                     region_name,
                                     partition_dt
                            ORDER BY mcc_group
                        ) xy
                    ) large2
                        ON large1.mcc = large2.mcc_group
                           AND large1.region_name = large2.region_name
                WHERE ABS(CAST((large1.trans_amount_on_terr - large2.trans_amount_on_terr)
                               / NULLIF(large1.trans_amount_on_terr, 0) * 100 AS NUMERIC(19, 2))
                         ) > 1
                      OR --Значения не вхдящие в интервал от -1% до 1%
                    ABS(CAST((large1.trans_amount_onl_other - large2.trans_amount_onl_other)
                             / NULLIF(large1.trans_amount_onl_other, 0) * 100 AS NUMERIC(19, 2))
                       ) > 1
                      OR ABS(CAST((large1.trans_amount_onl_rf - large2.trans_amount_onl_rf)
                                  / NULLIF(large1.trans_amount_onl_rf, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.trans_amount_other_countries - large2.trans_amount_other_countries)
                                  / NULLIF(large1.trans_amount_other_countries, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.trans_amount_other_region - large2.trans_amount_other_countries)
                                  / NULLIF(large1.trans_amount_other_region, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.trans_amount_other_terr - large2.trans_amount_other_countries)
                                  / NULLIF(large1.trans_amount_other_terr, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.count_trans_on_terr - large2.count_trans_on_terr)
                                  / NULLIF(large1.count_trans_on_terr, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.count_trans_onl_other - large2.count_trans_onl_other)
                                  / NULLIF(large1.count_trans_onl_other, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.count_trans_onl_rf - large2.count_trans_onl_rf)
                                  / NULLIF(large1.count_trans_onl_rf, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.count_trans_other_countries - large2.count_trans_other_countries)
                                  / NULLIF(large1.count_trans_other_countries, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.count_trans_other_region - large2.count_trans_other_region)
                                  / NULLIF(large1.count_trans_other_region, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.count_trans_other_terr - large2.count_trans_other_terr)
                                  / NULLIF(large1.count_trans_other_terr, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.client_cnt_on_terr - large2.client_cnt_on_terr)
                                  / NULLIF(large1.client_cnt_on_terr, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.client_cnt_onl_other - large2.client_cnt_onl_other)
                                  / NULLIF(large1.client_cnt_onl_other, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.client_cnt_onl_rf - large2.client_cnt_onl_rf)
                                  / NULLIF(large1.client_cnt_onl_rf, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.client_cnt_other_countries - large2.client_cnt_other_countries)
                                  / NULLIF(large1.client_cnt_other_countries, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.client_cnt_other_region - large2.client_cnt_other_region)
                                  / NULLIF(large1.client_cnt_other_region, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.client_cnt_other_terr - large2.client_cnt_other_terr)
                                  / NULLIF(large1.client_cnt_other_terr, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
            )
            BEGIN
                SET @k = @k + 1;
                SELECT large1.mcc AS 'СУММЫ ПО СГРУППИРОВАННЫМ ПО MCC_GROUP ЧИСЛОВЫМ КОДАМ НЕ СОВПАДАЮТ С СУММАМИ ПО ТЕКСТОВЫМ ГРУППАМ в FL_Trans (Отклонение, %)',
                       large1.region_name, --Демонстрация отклонений больше 1% илм меньше -1%
                       CAST((large1.trans_amount_on_terr - large2.trans_amount_on_terr)
                            / NULLIF(large1.trans_amount_on_terr, 0) * 100 AS NUMERIC(19, 2)) AS 'trans_amount_on_terr (deviation in %)',
                       CAST((large1.trans_amount_onl_other - large2.trans_amount_onl_other)
                            / NULLIF(large1.trans_amount_onl_other, 0) * 100 AS NUMERIC(19, 2)) AS 'trans_amount_onl_other_deviation (deviation in %)',
                       CAST((large1.trans_amount_onl_rf - large2.trans_amount_onl_rf)
                            / NULLIF(large1.trans_amount_onl_rf, 0) * 100 AS NUMERIC(19, 2)) AS 'trans_amount_onl_rf (deviation in %)',
                       CAST((large1.trans_amount_other_countries - large2.trans_amount_other_countries)
                            / NULLIF(large1.trans_amount_other_countries, 0) * 100 AS NUMERIC(19, 2)) AS 'trans_amount_other_countries(deviation in %)',
                       CAST((large1.trans_amount_other_region - large2.trans_amount_other_countries)
                            / NULLIF(large1.trans_amount_other_region, 0) * 100 AS NUMERIC(19, 2)) AS 'trans_amount_other_region (deviation in %)',
                       CAST((large1.trans_amount_other_terr - large2.trans_amount_other_countries)
                            / NULLIF(large1.trans_amount_other_terr, 0) * 100 AS NUMERIC(19, 2)) AS 'trans_amount_other_terr (deviation in %)',
                       CAST((large1.count_trans_on_terr - large2.count_trans_on_terr)
                            / NULLIF(large1.count_trans_on_terr, 0) * 100 AS NUMERIC(19, 2)) AS 'count_trans_on_terr (deviation in %)',
                       CAST((large1.count_trans_onl_other - large2.count_trans_onl_other)
                            / NULLIF(large1.count_trans_onl_other, 0) * 100 AS NUMERIC(19, 2)) AS 'count_trans_onl_other (deviation in %)',
                       CAST((large1.count_trans_onl_rf - large2.count_trans_onl_rf)
                            / NULLIF(large1.count_trans_onl_rf, 0) * 100 AS NUMERIC(19, 2)) AS 'count_trans_onl_rf (deviation in %)',
                       CAST((large1.count_trans_other_countries - large2.count_trans_other_countries)
                            / NULLIF(large1.count_trans_other_countries, 0) * 100 AS NUMERIC(19, 2)) AS 'count_trans_other_countries (deviation in %)',
                       CAST((large1.count_trans_other_region - large2.count_trans_other_region)
                            / NULLIF(large1.count_trans_other_region, 0) * 100 AS NUMERIC(19, 2)) AS 'count_trans_other_region (deviation in %)',
                       CAST((large1.count_trans_other_terr - large2.count_trans_other_terr)
                            / NULLIF(large1.count_trans_other_terr, 0) * 100 AS NUMERIC(19, 2)) AS 'count_trans_other_terr (deviation in %)',
                       CAST((large1.client_cnt_on_terr - large2.client_cnt_on_terr)
                            / NULLIF(large1.client_cnt_on_terr, 0) * 100 AS NUMERIC(19, 2)) AS 'client_cnt_on_terr (deviation in %)',
                       CAST((large1.client_cnt_onl_other - large2.client_cnt_onl_other)
                            / NULLIF(large1.client_cnt_onl_other, 0) * 100 AS NUMERIC(19, 2)) AS 'client_cnt_onl_other (deviation in %)',
                       CAST((large1.client_cnt_onl_rf - large2.client_cnt_onl_rf) / NULLIF(large1.client_cnt_onl_rf, 0)
                            * 100 AS NUMERIC(19, 2)) AS 'client_cnt_onl_rf (deviation in %)',
                       CAST((large1.client_cnt_other_countries - large2.client_cnt_other_countries)
                            / NULLIF(large1.client_cnt_other_countries, 0) * 100 AS NUMERIC(19, 2)) AS 'client_cnt_other_countries (deviation in %)',
                       CAST((large1.client_cnt_other_region - large2.client_cnt_other_region)
                            / NULLIF(large1.client_cnt_other_region, 0) * 100 AS NUMERIC(19, 2)) AS 'client_cnt_other_region (deviation in %)',
                       CAST((large1.client_cnt_other_terr - large2.client_cnt_other_terr)
                            / NULLIF(large1.client_cnt_other_terr, 0) * 100 AS NUMERIC(19, 2)) AS 'client_cnt_other_terr (deviation in %)',
                       large1.partition_dt
                FROM
                (
                    SELECT * --Расчёт отклонений показателей у неравных текстовых групп
                    FROM
                    (
                        SELECT TOP 1000
                            mcc,
                            region_name,
                            SUM(trans_amount_on_terr) AS trans_amount_on_terr,
                            SUM(trans_amount_onl_other) AS trans_amount_onl_other,
                            SUM(trans_amount_onl_rf) AS trans_amount_onl_rf,
                            SUM(trans_amount_other_countries) AS trans_amount_other_countries,
                            SUM(trans_amount_other_region) AS trans_amount_other_region,
                            SUM(trans_amount_other_terr) AS trans_amount_other_terr,
                            SUM(count_trans_on_terr) AS count_trans_on_terr,
                            SUM(count_trans_onl_other) AS count_trans_onl_other,
                            SUM(count_trans_onl_rf) AS count_trans_onl_rf,
                            SUM(count_trans_other_countries) AS count_trans_other_countries,
                            SUM(count_trans_other_region) AS count_trans_other_region,
                            SUM(count_trans_other_terr) AS count_trans_other_terr,
                            SUM(client_cnt_on_terr) AS client_cnt_on_terr,
                            SUM(client_cnt_onl_other) AS client_cnt_onl_other,
                            SUM(client_cnt_onl_rf) AS client_cnt_onl_rf,
                            SUM(client_cnt_other_countries) AS client_cnt_other_countries,
                            SUM(client_cnt_other_region) AS client_cnt_other_region,
                            SUM(client_cnt_other_terr) AS client_cnt_other_terr,
                            partition_dt
                        FROM Sber.Upload_csv
                        WHERE ISNUMERIC(mcc) != 1
                              AND partition_dt = @actdate
                              AND mcc IS NOT NULL --Текстовые группы
                        GROUP BY mcc,
                                 region_name,
                                 partition_dt
                        ORDER BY mcc
                    ) x
                    EXCEPT
                    SELECT *
                    FROM
                    (
                        SELECT TOP 1000
                            mcc_group,
                            region_name,
                            SUM(trans_amount_on_terr) AS trans_amount_on_terr,
                            SUM(trans_amount_onl_other) AS trans_amount_onl_other,
                            SUM(trans_amount_onl_rf) AS trans_amount_onl_rf,
                            SUM(trans_amount_other_countries) AS trans_amount_other_countries,
                            SUM(trans_amount_other_region) AS trans_amount_other_region,
                            SUM(trans_amount_other_terr) AS trans_amount_other_terr,
                            SUM(count_trans_on_terr) AS count_trans_on_terr,
                            SUM(count_trans_onl_other) AS count_trans_onl_other,
                            SUM(count_trans_onl_rf) AS count_trans_onl_rf,
                            SUM(count_trans_other_countries) AS count_trans_other_countries,
                            SUM(count_trans_other_region) AS count_trans_other_region,
                            SUM(count_trans_other_terr) AS count_trans_other_terr,
                            SUM(client_cnt_on_terr) AS client_cnt_on_terr,
                            SUM(client_cnt_onl_other) AS client_cnt_onl_other,
                            SUM(client_cnt_onl_rf) AS client_cnt_onl_rf,
                            SUM(client_cnt_other_countries) AS client_cnt_other_countries,
                            SUM(client_cnt_other_region) AS client_cnt_other_region,
                            SUM(client_cnt_other_terr) AS client_cnt_other_terr,
                            partition_dt
                        FROM
                        (
                            SELECT *
                            FROM
                            (
                                SELECT mcc,
                                       region_name,
                                       SUM(trans_amount_on_terr) AS trans_amount_on_terr,
                                       SUM(trans_amount_onl_other) AS trans_amount_onl_other,
                                       SUM(trans_amount_onl_rf) AS trans_amount_onl_rf,
                                       SUM(trans_amount_other_countries) AS trans_amount_other_countries,
                                       SUM(trans_amount_other_region) AS trans_amount_other_region,
                                       SUM(trans_amount_other_terr) AS trans_amount_other_terr,
                                       SUM(count_trans_on_terr) AS count_trans_on_terr,
                                       SUM(count_trans_onl_other) AS count_trans_onl_other,
                                       SUM(count_trans_onl_rf) AS count_trans_onl_rf,
                                       SUM(count_trans_other_countries) AS count_trans_other_countries,
                                       SUM(count_trans_other_region) AS count_trans_other_region,
                                       SUM(count_trans_other_terr) AS count_trans_other_terr,
                                       SUM(client_cnt_on_terr) AS client_cnt_on_terr,
                                       SUM(client_cnt_onl_other) AS client_cnt_onl_other,
                                       SUM(client_cnt_onl_rf) AS client_cnt_onl_rf,
                                       SUM(client_cnt_other_countries) AS client_cnt_other_countries,
                                       SUM(client_cnt_other_region) AS client_cnt_other_region,
                                       SUM(client_cnt_other_terr) AS client_cnt_other_terr,
                                       partition_dt
                                FROM Sber.Upload_csv
                                WHERE ISNUMERIC(mcc) = 1
                                      AND partition_dt = @actdate --Числовые коды
                                GROUP BY mcc,
                                         region_name,
                                         partition_dt
                            ) t1
                                LEFT JOIN Sber.MccGroup t2
                                    ON t1.mcc = t2.mcc_code
                        ) big
                        GROUP BY mcc_group,
                                 region_name,
                                 partition_dt
                        ORDER BY mcc_group
                    ) y
                ) large1
                    LEFT JOIN
                    (
                        SELECT *
                        FROM
                        (
                            SELECT TOP 1000
                                mcc_group,
                                region_name,
                                SUM(trans_amount_on_terr) AS trans_amount_on_terr,
                                SUM(trans_amount_onl_other) AS trans_amount_onl_other,
                                SUM(trans_amount_onl_rf) AS trans_amount_onl_rf,
                                SUM(trans_amount_other_countries) AS trans_amount_other_countries,
                                SUM(trans_amount_other_region) AS trans_amount_other_region,
                                SUM(trans_amount_other_terr) AS trans_amount_other_terr,
                                SUM(count_trans_on_terr) AS count_trans_on_terr,
                                SUM(count_trans_onl_other) AS count_trans_onl_other,
                                SUM(count_trans_onl_rf) AS count_trans_onl_rf,
                                SUM(count_trans_other_countries) AS count_trans_other_countries,
                                SUM(count_trans_other_region) AS count_trans_other_region,
                                SUM(count_trans_other_terr) AS count_trans_other_terr,
                                SUM(client_cnt_on_terr) AS client_cnt_on_terr,
                                SUM(client_cnt_onl_other) AS client_cnt_onl_other,
                                SUM(client_cnt_onl_rf) AS client_cnt_onl_rf,
                                SUM(client_cnt_other_countries) AS client_cnt_other_countries,
                                SUM(client_cnt_other_region) AS client_cnt_other_region,
                                SUM(client_cnt_other_terr) AS client_cnt_other_terr,
                                partition_dt
                            FROM
                            (
                                SELECT *
                                FROM
                                (
                                    SELECT mcc,
                                           region_name,
                                           SUM(trans_amount_on_terr) AS trans_amount_on_terr,
                                           SUM(trans_amount_onl_other) AS trans_amount_onl_other,
                                           SUM(trans_amount_onl_rf) AS trans_amount_onl_rf,
                                           SUM(trans_amount_other_countries) AS trans_amount_other_countries,
                                           SUM(trans_amount_other_region) AS trans_amount_other_region,
                                           SUM(trans_amount_other_terr) AS trans_amount_other_terr,
                                           SUM(count_trans_on_terr) AS count_trans_on_terr,
                                           SUM(count_trans_onl_other) AS count_trans_onl_other,
                                           SUM(count_trans_onl_rf) AS count_trans_onl_rf,
                                           SUM(count_trans_other_countries) AS count_trans_other_countries,
                                           SUM(count_trans_other_region) AS count_trans_other_region,
                                           SUM(count_trans_other_terr) AS count_trans_other_terr,
                                           SUM(client_cnt_on_terr) AS client_cnt_on_terr,
                                           SUM(client_cnt_onl_other) AS client_cnt_onl_other,
                                           SUM(client_cnt_onl_rf) AS client_cnt_onl_rf,
                                           SUM(client_cnt_other_countries) AS client_cnt_other_countries,
                                           SUM(client_cnt_other_region) AS client_cnt_other_region,
                                           SUM(client_cnt_other_terr) AS client_cnt_other_terr,
                                           partition_dt
                                    FROM Sber.Upload_csv
                                    WHERE ISNUMERIC(mcc) = 1
                                          AND partition_dt = @actdate --Числовые коды
                                    GROUP BY mcc,
                                             region_name,
                                             partition_dt
                                ) t1
                                    LEFT JOIN Sber.MccGroup t2
                                        ON t1.mcc = t2.mcc_code
                            ) big
                            GROUP BY mcc_group,
                                     region_name,
                                     partition_dt
                            ORDER BY mcc_group
                        ) xy
                    ) large2
                        ON large1.mcc = large2.mcc_group
                           AND large1.region_name = large2.region_name
                WHERE ABS(CAST((large1.trans_amount_on_terr - large2.trans_amount_on_terr)
                               / NULLIF(large1.trans_amount_on_terr, 0) * 100 AS NUMERIC(19, 2))
                         ) > 1
                      OR --Значения не вхдящие в интервал от -1% до 1%
                    ABS(CAST((large1.trans_amount_onl_other - large2.trans_amount_onl_other)
                             / NULLIF(large1.trans_amount_onl_other, 0) * 100 AS NUMERIC(19, 2))
                       ) > 1
                      OR ABS(CAST((large1.trans_amount_onl_rf - large2.trans_amount_onl_rf)
                                  / NULLIF(large1.trans_amount_onl_rf, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.trans_amount_other_countries - large2.trans_amount_other_countries)
                                  / NULLIF(large1.trans_amount_other_countries, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.trans_amount_other_region - large2.trans_amount_other_countries)
                                  / NULLIF(large1.trans_amount_other_region, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.trans_amount_other_terr - large2.trans_amount_other_countries)
                                  / NULLIF(large1.trans_amount_other_terr, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.count_trans_on_terr - large2.count_trans_on_terr)
                                  / NULLIF(large1.count_trans_on_terr, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.count_trans_onl_other - large2.count_trans_onl_other)
                                  / NULLIF(large1.count_trans_onl_other, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.count_trans_onl_rf - large2.count_trans_onl_rf)
                                  / NULLIF(large1.count_trans_onl_rf, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.count_trans_other_countries - large2.count_trans_other_countries)
                                  / NULLIF(large1.count_trans_other_countries, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.count_trans_other_region - large2.count_trans_other_region)
                                  / NULLIF(large1.count_trans_other_region, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.count_trans_other_terr - large2.count_trans_other_terr)
                                  / NULLIF(large1.count_trans_other_terr, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.client_cnt_on_terr - large2.client_cnt_on_terr)
                                  / NULLIF(large1.client_cnt_on_terr, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.client_cnt_onl_other - large2.client_cnt_onl_other)
                                  / NULLIF(large1.client_cnt_onl_other, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.client_cnt_onl_rf - large2.client_cnt_onl_rf)
                                  / NULLIF(large1.client_cnt_onl_rf, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.client_cnt_other_countries - large2.client_cnt_other_countries)
                                  / NULLIF(large1.client_cnt_other_countries, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.client_cnt_other_region - large2.client_cnt_other_region)
                                  / NULLIF(large1.client_cnt_other_region, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
                      OR ABS(CAST((large1.client_cnt_other_terr - large2.client_cnt_other_terr)
                                  / NULLIF(large1.client_cnt_other_terr, 0) * 100 AS NUMERIC(19, 2))
                            ) > 1
            END
        END

        --ПОДРУЗКА НОВЫХ СТОЛБЦОВ ПРИ АДЕКВАТНОСТИ ЗАГРУЖЕННОЙ ТАБЛИЦЫ--
        IF @k > -1
        BEGIN
            CREATE TABLE Sber.Part (partition_dt NVARCHAR(10) NULL);
            CREATE TABLE Sber.Uniq (partitiones NVARCHAR(10) NULL);
            INSERT INTO Sber.Part --Наложение друг под другом уникальных временных разрезов из новой и старой таблиц
            SELECT DISTINCT
                (partition_dt)
            FROM Sber.Upload_csv
            UNION ALL
            SELECT DISTINCT
                (partition_dt)
            FROM Sber.FL_Trans;
            INSERT INTO Sber.Uniq --Поиск временных разрезов, встречающихся в застакованной таблице только 1 раз
            SELECT partition_dt
            FROM
            (
                SELECT *,
                       COUNT(partition_dt) OVER (PARTITION BY partition_dt) as freq
                FROM Sber.Part
            ) sub
            WHERE sub.freq = 1
                  AND partition_dt NOT IN (
                                              SELECT DISTINCT (partition_dt) FROM Sber.FL_Trans
                                          );
            IF EXISTS (SELECT * FROM Sber.Uniq) --Проверка существования новых временных разрезов
            BEGIN
                INSERT INTO Sber.FL_Trans --Вставка строк из новой стаблицы с новыми временными разрезами в старую
                SELECT *
                FROM Sber.Upload_csv
                WHERE partition_dt IN (
                                          SELECT * FROM Sber.Uniq
                                      );
                DROP TABLE Sber.Filenames;
                DROP TABLE Sber.Part;
                DROP TABLE Sber.Uniq;
                DROP TABLE Sber.Upload_csv;
            END
            ELSE
            BEGIN
                SELECT ('.') AS 'НОВЫХ ВРЕМЕННЫХ РАЗРЕЗОВ НЕ ОБНАРУЖЕНО';
                DROP TABLE Sber.Filenames;
                DROP TABLE Sber.Part;
                DROP TABLE Sber.Uniq;
                DROP TABLE Sber.Upload_csv;
            END
        END
        ELSE
        BEGIN
            DROP TABLE Sber.Filenames;
            DROP TABLE Sber.Part;
            DROP TABLE Sber.Uniq;
            DROP TABLE Sber.Upload_csv;
        END
    END

    --ЧТЕНИЕ ФАЙЛА ПРО БЛАГОСОСТОЯНИЕ ФИЗИЧЕСКИХ ЛИЦ--
    IF CHARINDEX('fl_blago', @csvfile) > 0 --Cоздание схемы таблицы
    BEGIN
        CREATE TABLE Sber.Upload_csv
        (
            region_name NVARCHAR(35) NULL,
            ao_name NVARCHAR(75) NULL,
            district_name NVARCHAR(59) NULL,
            gender NVARCHAR(1) NULL,
            age_group SMALLINT NULL,
            income_group INT NULL,
            client_cnt_in_cluster INT NULL,
            kids_ratio NUMERIC(5, 4) NULL,
            pensions_ratio NUMERIC(5, 4) NULL,
            self_ratio NUMERIC(5, 4) NULL,
            salary_ratio NUMERIC(5, 4) NULL,
            stipend_ratio NUMERIC(5, 4) NULL,
            pension_ratio NUMERIC(5, 4) NULL,
            dividend_ratio NUMERIC(5, 4) NULL,
            unemployment_ratio NUMERIC(5, 4) NULL,
            pregnancy_ratio NUMERIC(5, 4) NULL,
            additional_payment_ratio NUMERIC(5, 4) NULL,
            avg_all_rur NUMERIC(19, 2) NULL,
            avg_salary_rur NUMERIC(19, 2) NULL,
            avg_pension_rur NUMERIC(19, 2) NULL,
            avg_stipend_rur NUMERIC(19, 2) NULL,
            avg_dividend_rur NUMERIC(19, 2) NULL,
            avg_unemployment_rur NUMERIC(19, 2) NULL,
            avg_pregnancy_rur NUMERIC(19, 2) NULL,
            avg_additional_payment_rur NUMERIC(19, 2) NULL,
            avg_deposit NUMERIC(19, 2) NULL,
            avg_cards_check NUMERIC(19, 2) NULL,
            avg_dovosstr NUMERIC(19, 2) NULL,
            avg_trans_amount NUMERIC(19, 2) NULL,
            avg_deposit_ratio NUMERIC(5, 4) NULL,
            avg_cards_check_ratio NUMERIC(5, 4) NULL,
            avg_dovosstr_ratio NUMERIC(5, 4) NULL,
            partition_dt NVARCHAR(10) NULL
        ); --Cоздание схемы таблицы
        SELECT @opener
            = 'BULK INSERT Sber.Upload_csv FROM ' + quotename(@filepath, '''')
              + --Считывание файла с диска
            'WITH (FIELDTERMINATOR = '';'',
                   ROWTERMINATOR = ''0x0A'',
                   CODEPAGE = ''65001'',
                   FIRSTROW = 2,
                   FIELDQUOTE = ''"'',
                   FORMAT = ''CSV'')';
        EXEC sp_executesql @opener;

        --ПРОВЕРКА ТАБЛИЦЫ ПРО БЛАГОСОСТОЯНИЕ ФИЗИЧЕСКИХ ЛИЦ--
        SET @k = 0; --Счётчик несоответствия признаков
        IF EXISTS
        (
            SELECT * --Проверка истинности вхождения в доходную группу
            FROM Sber.Upload_csv
            WHERE avg_all_rur > income_group
                  AND partition_dt LIKE '%-%'
                  AND income_group != 2000001
        )
        BEGIN
            SET @k = @k + 1;
            SELECT t.region_name AS 'НЕ СХОДЯТСЯ СОВОКУПНЫЕ ДОХОДЫ С ГРАНИЦАМИ ДОХОДОВ В "FL"',
                   t.* --Вывод таблицы с кластерами, чьи доходы не входят в доходную группу
            FROM Sber.Upload_csv t
            WHERE avg_all_rur > income_group
                  AND partition_dt LIKE '%-%'
                  AND income_group != 2000001;
        END
        IF EXISTS
        (
            SELECT CAST((scource_income - all_income) / scource_income * 100 AS NUMERIC(19, 2)) -- Проверка тех кластеров, где сумма по источникам дохода не равна сумме совокупного дохода
                AS deviation,
                   columnes.*
            FROM
            (
                SELECT a.*,
                       CAST(avg_salary_rur * client_cnt_in_cluster * salary_ratio + avg_pension_rur
                            * client_cnt_in_cluster * pension_ratio + avg_stipend_rur * client_cnt_in_cluster
                            * stipend_ratio + avg_dividend_rur * client_cnt_in_cluster * dividend_ratio
                            + avg_unemployment_rur * client_cnt_in_cluster * unemployment_ratio + avg_pregnancy_rur
                            * client_cnt_in_cluster * pregnancy_ratio + avg_additional_payment_rur
                            * client_cnt_in_cluster * additional_payment_ratio AS NUMERIC(19, 2)) AS scource_income,
                       client_cnt_in_cluster * avg_all_rur AS all_income
                FROM Sber.Upload_csv a
            ) columnes
            WHERE scource_income != 0
                  AND ABS((scource_income - all_income) / scource_income * 100) > 1
                  AND partition_dt LIKE '%-%'
        )
        BEGIN
            SET @k = @k + 1;
            SELECT CAST((scource_income - all_income) / scource_income * 100 AS NUMERIC(19, 2)) AS 'Отклонение,% (НЕ СХОДИТСЯ СУММА ПО ИСТОЧНИКАМ ДОХОДОВ С СОВОКУПНОЙ СУММОЙ ДОХОДОВ В "FL"',
                   columnes.*
            FROM
            (
                SELECT a.*,
                       CAST(avg_salary_rur * client_cnt_in_cluster * salary_ratio
                            + --Вывод таблицы с теми кластерам, где сумма по источникам дохода не равна сумме совокупного дохода
                       avg_pension_rur * client_cnt_in_cluster * pension_ratio + avg_stipend_rur
                            * client_cnt_in_cluster * stipend_ratio + avg_dividend_rur * client_cnt_in_cluster
                            * dividend_ratio + avg_unemployment_rur * client_cnt_in_cluster * unemployment_ratio
                            + avg_pregnancy_rur * client_cnt_in_cluster * pregnancy_ratio + avg_additional_payment_rur
                            * client_cnt_in_cluster * additional_payment_ratio AS NUMERIC(19, 2)) AS scource_income,
                       client_cnt_in_cluster * avg_all_rur AS all_income
                FROM Sber.Upload_csv a
            ) columnes
            WHERE scource_income != 0
                  AND ABS((scource_income - all_income) / scource_income * 100) > 1
                  AND partition_dt LIKE '%-%';
        END

        --ПОДРУЗКА НОВЫХ СТОЛБЦОВ ПРИ АДЕКВАТНОСТИ ЗАГРУЖЕННОЙ ТАБЛИЦЫ--
        IF @k > -1
        BEGIN
            CREATE TABLE Sber.Part (partition_dt NVARCHAR(10) NULL);
            CREATE TABLE Sber.Uniq (partitiones NVARCHAR(10) NULL);
            INSERT INTO Sber.Part --Наложение друг под другом уникальных временных разрезов из новой и старой таблиц
            SELECT DISTINCT
                (partition_dt)
            FROM Sber.Upload_csv
            UNION ALL
            SELECT DISTINCT
                (partition_dt)
            FROM Sber.FL;
            INSERT INTO Sber.Uniq --Поиск временных разрезов, встречающихся в застакованной таблице только 1 раз
            SELECT partition_dt
            FROM
            (
                SELECT *,
                       COUNT(partition_dt) OVER (PARTITION BY partition_dt) as freq
                FROM Sber.Part
            ) sub
            WHERE sub.freq = 1
                  AND partition_dt NOT IN (
                                              SELECT DISTINCT (partition_dt) FROM Sber.FL
                                          );
            IF EXISTS (SELECT * FROM Sber.Uniq) --Проверка существования новых временных разрезов
            BEGIN
                INSERT INTO Sber.FL --Вставка строк из новой стаблицы с новыми временными разрезами в старую
                SELECT *
                FROM Sber.Upload_csv
                WHERE partition_dt IN (
                                          SELECT * FROM Sber.Uniq
                                      );
                DROP TABLE Sber.Filenames;
                DROP TABLE Sber.Part;
                DROP TABLE Sber.Uniq;
                DROP TABLE Sber.Upload_csv;
            END
            ELSE
            BEGIN
                SELECT ('.') AS 'НОВЫХ ВРЕМЕННЫХ РАЗРЕЗОВ НЕ ОБНАРУЖЕНО';
                DROP TABLE Sber.Filenames;
                DROP TABLE Sber.Part;
                DROP TABLE Sber.Uniq;
                DROP TABLE Sber.Upload_csv;
            END
        END
        ELSE
        BEGIN
            DROP TABLE Sber.Filenames;
            DROP TABLE Sber.Part;
            DROP TABLE Sber.Uniq;
            DROP TABLE Sber.Upload_csv;
        END
    END

    --ЧТЕНИЕ ФАЙЛА ПРО ЮРИДИЧЕСКИХ ЛИЦ--
    IF CHARINDEX('ul_', @csvfile) > 0
    BEGIN
        CREATE TABLE Sber.Upload_csv
        (
            num SMALLINT NULL,
            region SMALLINT NULL,
            adm_district NUMERIC(20, 0) NULL,
            district NUMERIC(20, 0) NULL,
            [type] NVARCHAR(2) NULL,
            okved_chapter NVARCHAR(1) NULL,
            okved_class SMALLINT NULL,
            okved_subclass SMALLINT NULL,
            segment NVARCHAR(50) NULL,
            tax_type NVARCHAR(50) NULL,
            cnt_inn NUMERIC(20, 0) NULL,
            SUM_income NUMERIC(19, 2) NULL,
            ratio_SUM_income NUMERIC(5, 4) NULL,
            avg_SUM_dt NUMERIC(19, 2) NULL,
            ratio_SUM_dt NUMERIC(5, 4) NULL,
            avg_SUM_kt NUMERIC(19, 2) NULL,
            ratio_SUM_kt NUMERIC(5, 4) NULL,
            avg_income NUMERIC(19, 2) NULL,
            avg_employee_ip SMALLINT NULL,
            SUM_salary NUMERIC(19, 2) NULL,
            avg_salary NUMERIC(19, 2) NULL,
            avg_employee NUMERIC(20, 0) NULL,
            ratio_SUM_salary NUMERIC(5, 4) NULL,
            all_trans_amount NUMERIC(19, 2) NULL,
            all_trans_cnt NUMERIC(19, 2) NULL,
            all_uniq_client_cnt NUMERIC(19, 2) NULL,
            all_avg_cheque NUMERIC(19, 2) NULL,
            ratio_all_trans NUMERIC(5, 4) NULL,
            online_trans_amount NUMERIC(19, 2) NULL,
            online_trans_cnt NUMERIC(19, 2) NULL,
            online_uniq_client_cnt NUMERIC(19, 2) NULL,
            online_avg_cheque NUMERIC(19, 2) NULL,
            ratio_online_trans NUMERIC(5, 4) NULL,
            partition_dt NVARCHAR(10) NULL
        ); --Cоздание схемы таблицы
        SELECT @opener
            = 'BULK INSERT Sber.Upload_csv FROM ' + quotename(@filepath, '''')
              + --Считывание файла с диска
            'WITH (FIELDTERMINATOR = '';'',
                   ROWTERMINATOR = ''0x0A'',
                   CODEPAGE = ''65001'',
                   FIRSTROW = 2,
                   FIELDQUOTE = ''"'',
                   FORMAT = ''CSV'')';
        EXEC sp_executesql @opener;

        --ПРОВЕРКА ТАБЛИЦЫ ПРО ЮРИДИЧЕСКИХ ЛИЦ--
        SET @k = 0; --Счётчик несоответствия признаков
        IF EXISTS
        (
            SELECT * --Проверка соответствия совокупного дохода юридических лиц
            FROM Sber.Upload_csv
            WHERE cnt_inn * avg_income != sum_income
                  AND partition_dt LIKE '%-%'
        )
        BEGIN
            IF EXISTS
            (
                SELECT (sum_income - cnt_inn * avg_income) / sum_income * 100 AS deviation,
                       * -- Проверка наличия строк с отклонения более 1% по модулю
                FROM Sber.Upload_csv
                WHERE partition_dt LIKE '%-%'
                      AND ABS((sum_income - cnt_inn * avg_income) / sum_income * 100) > 1
            )
            BEGIN
                SET @k = @k + 1;
                SELECT CAST((sum_income - cnt_inn * avg_income) / sum_income * 100 AS NUMERIC(19, 2)) AS 'СУММЫ СОВОКУПНЫХ ДОХОДОВ НЕ СХОДЯТСЯ CО СВЯЗАННЫМИ ПОКАЗАТЕЛЯМИ В UL (ОТКЛОНЕНИЕ, %)',
                       *
                FROM Sber.Upload_csv
                WHERE partition_dt LIKE '%-%'
                      AND ABS((sum_income - cnt_inn * avg_income) / sum_income * 100) > 1;
            END
        END
        IF EXISTS
        (
            SELECT * --Проверка соответствия совокупной суммы транзакций юридических лиц
            FROM Sber.Upload_csv
            WHERE all_avg_cheque * all_trans_cnt != all_trans_amount
                  AND partition_dt LIKE '%-%'
        )
        BEGIN
            IF EXISTS
            (
                SELECT (all_trans_amount - all_avg_cheque * all_trans_cnt) / all_trans_amount * 100 -- Проверка наличия строк с отклонения более 1% по модулю
                    AS deviation,
                       *
                FROM Sber.Upload_csv
                WHERE partition_dt LIKE '%-%'
                      AND ABS((all_trans_amount - all_avg_cheque * all_trans_cnt) / all_trans_amount * 100) > 1
            )
            BEGIN
                SET @k = @k + 1;
                SELECT CAST((all_trans_amount - all_avg_cheque * all_trans_cnt) / all_trans_amount * 100 AS NUMERIC(19, 2)) AS 'СУММЫ СОВОКУПНЫХ ТРАНЗАКЦИЙ НЕ СХОДЯТСЯ CО СВЯЗАННЫМИ ПОКАЗАТЕЛЯМИ В UL (ОТКЛОНЕНИЕ, %)',
                       *
                FROM Sber.Upload_csv
                WHERE partition_dt LIKE '%-%'
                      AND ABS((all_trans_amount - all_avg_cheque * all_trans_cnt) / all_trans_amount * 100) > 1;
            END
        END

        --ПОДРУЗКА НОВЫХ СТОЛБОЦОВ ПРИ АДЕКВАТНОСТИ ЗАГРУЖЕННОЙ ТАБЛИЦЫ--
        IF @k > -1
        BEGIN
            CREATE TABLE Sber.Part (partition_dt NVARCHAR(10) NULL);
            CREATE TABLE Sber.Uniq (partitiones NVARCHAR(10) NULL);
            INSERT INTO Sber.Part --Наложение друг под другом уникальных временных разрезов из новой и старой таблиц
            SELECT DISTINCT (partition_dt)
            FROM Sber.Upload_csv
            UNION ALL
            SELECT DISTINCT (partition_dt)
            FROM Sber.UL;
            INSERT INTO Sber.Uniq --Поиск временных разрезов, встречающихся в застакованной таблице только 1 раз
            SELECT partition_dt
            FROM
            (
                SELECT *,
                       COUNT(partition_dt) OVER (PARTITION BY partition_dt) as freq
                FROM Sber.Part
            ) sub
            WHERE sub.freq = 1
                  AND partition_dt NOT IN (
                                              SELECT DISTINCT (partition_dt) FROM Sber.UL
                                          );
            IF EXISTS (SELECT * FROM Sber.Uniq) --Проверка существования новых временных разрезов
            BEGIN
                INSERT INTO Sber.UL --Вставка строк из новой стаблицы с новыми временными разрезами в старую
                SELECT *
                FROM Sber.Upload_csv
                WHERE partition_dt IN (
                                          SELECT * FROM Sber.Uniq
                                      );
                DROP TABLE Sber.Filenames;
                DROP TABLE Sber.Part;
                DROP TABLE Sber.Uniq
                DROP TABLE Sber.Upload_csv;
            END
            ELSE
            BEGIN
                SELECT ('.') AS 'НОВЫХ ВРЕМЕННЫХ РАЗРЕЗОВ НЕ ОБНАРУЖЕНО';
                DROP TABLE Sber.Filenames;
                DROP TABLE Sber.Part;
                DROP TABLE Sber.Uniq;
                DROP TABLE Sber.Upload_csv;
            END
        END
        ELSE
        BEGIN
            DROP TABLE Sber.Filenames;
            DROP TABLE Sber.Part;
            DROP TABLE Sber.Uniq;
            DROP TABLE Sber.Upload_csv;
        END
    END
END