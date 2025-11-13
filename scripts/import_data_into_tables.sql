/*
=============================================================================
--- SCRIPT ETL TỔNG HỢP
=============================================================================
*/

-- 1. ĐẢM BẢO SỬ DỤNG ĐÚNG DATABASE
USE HeartDiseaseDB;
GO

SET NOCOUNT ON;

-- PHẦN 2: BẮT ĐẦU QUÁ TRÌNH ETL 
-- -------------------------------------------------------------------
PRINT N'Bắt đầu quá trình ETL...';

BEGIN TRY
    BEGIN TRANSACTION;
    PRINT N'Đang nạp dữ liệu vào dbo.DimPerson...';
    INSERT INTO dbo.DimPerson (Sex, AgeCategory, RaceEthnicityCategory)
    SELECT DISTINCT
        ISNULL(R.Sex, 'Unknown') AS Sex,
        ISNULL(R.AgeCategory, 'Unknown') AS AgeCategory,
        ISNULL(R.RaceEthnicityCategory, 'Unknown') AS RaceEthnicityCategory
    FROM dbo.HeartDiseaseCleaned AS R;


    PRINT N'Đang nạp dữ liệu vào dbo.DimState...';
    INSERT INTO dbo.DimState (StateName, Region)
    SELECT DISTINCT
        ISNULL(R.State, 'Unknown') AS StateName,
        -- CẬP NHẬT LOGIC (V5): Ánh xạ Region từ StateName
        CASE
            WHEN R.State IN (
                'Alabama', 'Arkansas', 'Florida', 'Georgia', 'Kentucky', 'Louisiana', 
                'Mississippi', 'North Carolina', 'South Carolina', 'Tennessee', 
                'Virginia', 'West Virginia'
            ) THEN 'Southeast'
            WHEN R.State IN (
                'Alaska', 'California', 'Colorado', 'Hawaii', 'Idaho', 'Montana', 
                'Nevada', 'Oregon', 'Utah', 'Washington', 'Wyoming'
            ) THEN 'West'
            WHEN R.State IN (
                'Connecticut', 'Delaware', 'Maine', 'Maryland', 'Massachusetts', 
                'New Hampshire', 'New Jersey', 'New York', 'Pennsylvania', 
                'Rhode Island', 'Vermont'
            ) THEN 'Northeast'
            WHEN R.State IN (
                'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Michigan', 'Minnesota', 
                'Missouri', 'Nebraska', 'North Dakota', 'Ohio', 'South Dakota', 
                'Wisconsin'
            ) THEN 'Midwest'
            WHEN R.State IN (
                'Arizona', 'New Mexico', 'Oklahoma', 'Texas'
            ) THEN 'Southwest'
            ELSE NULL -- Cho 'Guam', 'Puerto Rico', 'Virgin Islands', 'Unknown'
        END AS Region
    FROM dbo.HeartDiseaseCleaned AS R;


    PRINT N'Đang nạp dữ liệu vào dbo.DimCheckupTime...';
    INSERT INTO dbo.DimCheckupTime (LastCheckupTime, CheckupRecency)
    SELECT DISTINCT
        ISNULL(R.LastCheckupTime, 'Unknown') AS LastCheckupTime,
        CASE
            WHEN R.LastCheckupTime = 'Within past year (anytime less than 12 months ago)' THEN 1
            WHEN R.LastCheckupTime = 'Within past 2 years (1 year but less than 2 years ago)' THEN 2
            WHEN R.LastCheckupTime = 'Within past 5 years (2 years but less than 5 years ago)' THEN 3
            WHEN R.LastCheckupTime = '5 or more years ago' THEN 4
            ELSE 0
        END AS CheckupRecency
    FROM dbo.HeartDiseaseCleaned AS R;


    PRINT N'Đang nạp dữ liệu vào dbo.DimPhysicalActivity...';
    INSERT INTO dbo.DimPhysicalActivity (PhysicalActivities, ActivityLevel)
    SELECT DISTINCT
        ISNULL(R.PhysicalActivities, 'Unknown') AS PhysicalActivities,
        CASE
            WHEN R.PhysicalActivities = 'Yes' THEN 'Active'
            WHEN R.PhysicalActivities = 'No' THEN 'Inactive'
            ELSE 'Unknown'
        END AS ActivityLevel
    FROM dbo.HeartDiseaseCleaned AS R;


    PRINT N'Đang nạp dữ liệu vào dbo.DimLifestyle...';
    INSERT INTO dbo.DimLifestyle (SmokerStatus, ECigaretteUsage, AlcoholDrinkers, SleepQuality)
    SELECT DISTINCT
        CASE
            WHEN R.SmokerStatus = 'Never smoked' THEN 'Never'
            WHEN R.SmokerStatus = 'Former smoker' THEN 'Former'
            WHEN R.SmokerStatus IN ('Current smoker - now smokes every day', 'Current smoker - now smokes some days') THEN 'Current (daily/some days)'
            ELSE 'Unknown'
        END AS SmokerStatus,
        CASE
            WHEN R.ECigaretteUsage IN ('Never used e-cigarettes in my entire life', 'Not at all (right now)') THEN 'Never'
            WHEN R.ECigaretteUsage = 'Use them some days' THEN 'Sometimes'
            WHEN R.ECigaretteUsage = 'Use them every day' THEN 'Every day'
            ELSE 'Unknown'
        END AS ECigaretteUsage,
        ISNULL(R.AlcoholDrinkers, 'No') AS AlcoholDrinkers,
        CASE
            WHEN R.SleepHours < 6 THEN 'Poor'
            WHEN R.SleepHours >= 6 AND R.SleepHours <= 8 THEN 'Good'
            WHEN R.SleepHours > 8 THEN 'Excessive'
            ELSE 'Unknown'
        END AS SleepQuality
    FROM dbo.HeartDiseaseCleaned AS R;


    PRINT N'Đang nạp dữ liệu vào dbo.DimChronicDiseases...';
    INSERT INTO dbo.DimChronicDiseases (
        HadDiabetes, HadArthritis, HadCOPD, HadKidneyDisease,
        HadDepressiveDisorder, HadCancer, HadSkinCancer
    )
    SELECT DISTINCT
        CASE
            WHEN R.HadDiabetes IN ('Yes', 'Yes (but only during pregnancy - female)') THEN 'Yes'
            WHEN R.HadDiabetes IN ('No', 'No (pre-diabetes or borderline diabetes)') THEN 'No'
            ELSE 'No'
        END AS HadDiabetes,
        ISNULL(R.HadArthritis, 'No') AS HadArthritis,
        ISNULL(R.HadCOPD, 'No') AS HadCOPD,
        ISNULL(R.HadKidneyDisease, 'No') AS HadKidneyDisease,
        ISNULL(R.HadDepressiveDisorder, 'No') AS HadDepressiveDisorder,
        CASE WHEN ISNULL(R.HadSkinCancer, 'No') = 'Yes' THEN 'Yes' ELSE 'No' END AS HadCancer,
        ISNULL(R.HadSkinCancer, 'No') AS HadSkinCancer
    FROM dbo.HeartDiseaseCleaned AS R;


    -- 3. POPULATE BẢNG FACT (HEALTHRECORD)
    PRINT N'Đang chuẩn bị và nạp dữ liệu vào bảng Fact dbo.HealthRecord...';

    ;WITH RawTransformed AS (
        SELECT
            R.PhysicalHealthDays,
            R.MentalHealthDays,
            R.SleepHours,
            R.HeightInMeters,
            R.WeightInKilograms,
            R.BMI,
            CASE
                WHEN R.HadHeartAttack = 'Yes' OR R.HadAngina = 'Yes' OR R.HadStroke = 'Yes' THEN 1
                ELSE 0
            END AS HeartDiseaseFlag,
            ISNULL(R.Sex, 'Unknown') AS Sex,
            ISNULL(R.AgeCategory, 'Unknown') AS AgeCategory,
            ISNULL(R.RaceEthnicityCategory, 'Unknown') AS RaceEthnicityCategory,
            ISNULL(R.State, 'Unknown') AS StateName,
            ISNULL(R.LastCheckupTime, 'Unknown') AS LastCheckupTime,
            ISNULL(R.PhysicalActivities, 'Unknown') AS PhysicalActivities,
            ISNULL(R.AlcoholDrinkers, 'No') AS AlcoholDrinkers,
            CASE
                WHEN R.SmokerStatus = 'Never smoked' THEN 'Never'
                WHEN R.SmokerStatus = 'Former smoker' THEN 'Former'
                WHEN R.SmokerStatus IN ('Current smoker - now smokes every day', 'Current smoker - now smokes some days') THEN 'Current (daily/some days)'
                ELSE 'Unknown'
            END AS SmokerStatus,
            CASE
                WHEN R.ECigaretteUsage IN ('Never used e-cigarettes in my entire life', 'Not at all (right now)') THEN 'Never'
                WHEN R.ECigaretteUsage = 'Use them some days' THEN 'Sometimes'
                WHEN R.ECigaretteUsage = 'Use them every day' THEN 'Every day'
                ELSE 'Unknown'
            END AS ECigaretteUsage,
            CASE
                WHEN R.SleepHours < 6 THEN 'Poor'
                WHEN R.SleepHours >= 6 AND R.SleepHours <= 8 THEN 'Good'
                WHEN R.SleepHours > 8 THEN 'Excessive'
                ELSE 'Unknown'
            END AS SleepQuality,
            CASE
                WHEN R.HadDiabetes IN ('Yes', 'Yes (but only during pregnancy - female)') THEN 'Yes'
                WHEN R.HadDiabetes IN ('No', 'No (pre-diabetes or borderline diabetes)') THEN 'No'
                ELSE 'No'
            END AS HadDiabetes,
            ISNULL(R.HadArthritis, 'No') AS HadArthritis,
            ISNULL(R.HadCOPD, 'No') AS HadCOPD,
            ISNULL(R.HadKidneyDisease, 'No') AS HadKidneyDisease,
            ISNULL(R.HadDepressiveDisorder, 'No') AS HadDepressiveDisorder,
            CASE WHEN ISNULL(R.HadSkinCancer, 'No') = 'Yes' THEN 'Yes' ELSE 'No' END AS HadCancer,
            ISNULL(R.HadSkinCancer, 'No') AS HadSkinCancer
        FROM
            dbo.HeartDiseaseCleaned AS R
    )
    -- Bước 3.2: INSERT vào bảng Fact bằng cách JOIN CTE với các bảng Dim
    INSERT INTO dbo.HealthRecord (
        PersonID, StateID, CheckupTimeID, PhysicalActivityID,
        ChronicDiseaseID, LifestyleID, HeartDiseaseFlag,
        PhysicalHealthDays, MentalHealthDays, SleepHours,
        HeightInMeters, WeightInKilograms, BMI, RecordYear
    )
    SELECT
        P.PersonID,
        S.StateID,
        CT.CheckupTimeID,
        PA.PhysicalActivityID,
        CD.ChronicDiseaseID,
        L.LifestyleID,
        RT.HeartDiseaseFlag,
        RT.PhysicalHealthDays,
        RT.MentalHealthDays,
        RT.SleepHours,
        RT.HeightInMeters,
        RT.WeightInKilograms,
        RT.BMI,
        2022 AS RecordYear
    FROM
        RawTransformed AS RT
    INNER JOIN dbo.DimPerson AS P
        ON RT.Sex = P.Sex
        AND RT.AgeCategory = P.AgeCategory
        AND RT.RaceEthnicityCategory = P.RaceEthnicityCategory
    INNER JOIN dbo.DimState AS S
        ON RT.StateName = S.StateName
    INNER JOIN dbo.DimCheckupTime AS CT
        ON RT.LastCheckupTime = CT.LastCheckupTime
    INNER JOIN dbo.DimPhysicalActivity AS PA
        ON RT.PhysicalActivities = PA.PhysicalActivities
    INNER JOIN dbo.DimLifestyle AS L
        ON RT.SmokerStatus = L.SmokerStatus
        AND RT.ECigaretteUsage = L.ECigaretteUsage
        AND RT.AlcoholDrinkers = L.AlcoholDrinkers
        AND RT.SleepQuality = L.SleepQuality
    INNER JOIN dbo.DimChronicDiseases AS CD
        ON RT.HadDiabetes = CD.HadDiabetes
        AND RT.HadArthritis = CD.HadArthritis
        AND RT.HadCOPD = CD.HadCOPD
        AND RT.HadKidneyDisease = CD.HadKidneyDisease
        AND RT.HadDepressiveDisorder = CD.HadDepressiveDisorder
        AND RT.HadCancer = CD.HadCancer
        AND RT.HadSkinCancer = CD.HadSkinCancer;

    COMMIT TRANSACTION;
    PRINT N'--- HOÀN THÀNH QUÁ TRÌNH ETL! ---';

END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    PRINT N'--- ĐÃ XẢY RA LỖI! ---';
    PRINT N'Transaction đã được Rollback.';
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
    DECLARE @ErrorState INT = ERROR_STATE();

    PRINT N'Lỗi: ' + @ErrorMessage;
    RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState);
END CATCH
GO