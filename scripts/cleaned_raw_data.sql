USE HeartDiseaseDB;
GO


DECLARE @Schema SYSNAME     = N'dbo';
DECLARE @RawTable SYSNAME   = N'HeartDiseaseRaw';
DECLARE @CleanTable SYSNAME = N'HeartDiseaseCleaned';
DECLARE @FilePath NVARCHAR(4000) = N'C:\Users\Windows\Desktop\Zamaha_Lab\DAT111_project\data\processed\final_data.csv';
DECLARE @AutoImport BIT = 1;  


IF OBJECT_ID(QUOTENAME(@Schema) + N'.' + QUOTENAME(@RawTable),'U') IS NULL
BEGIN
    IF @AutoImport = 0
    BEGIN
        RAISERROR('Ch?a c� b?ng %s.%s. B?t @AutoImport = 1 ho?c t?o tr??c khi ch?y.gt',
                  16, 1, @Schema, @RawTable);
        RETURN;
    END


    DECLARE @sql NVARCHAR(MAX) = N'
    CREATE TABLE ' + QUOTENAME(@Schema) + N'.' + QUOTENAME(@RawTable) + N'(
        State NVARCHAR(50),
        Sex NVARCHAR(10),
        GeneralHealth NVARCHAR(30),
        PhysicalHealthDays FLOAT,
        MentalHealthDays FLOAT,
        LastCheckupTime NVARCHAR(150),
        PhysicalActivities NVARCHAR(5),
        SleepHours FLOAT,
        RemovedTeeth NVARCHAR(50),
        HadHeartAttack NVARCHAR(5),
        HadAngina NVARCHAR(5),
        HadStroke NVARCHAR(5),
        HadAsthma NVARCHAR(5),
        HadSkinCancer NVARCHAR(5),
        HadCOPD NVARCHAR(5),
        HadDepressiveDisorder NVARCHAR(5),
        HadKidneyDisease NVARCHAR(5),
        HadArthritis NVARCHAR(5),
        HadDiabetes NVARCHAR(100),
        DeafOrHardOfHearing NVARCHAR(5),
        BlindOrVisionDifficulty NVARCHAR(5),
        DifficultyConcentrating NVARCHAR(5),
        DifficultyWalking NVARCHAR(3),
        DifficultyDressingBathing NVARCHAR(3),
        DifficultyErrands NVARCHAR(5),
        SmokerStatus NVARCHAR(100),
        ECigaretteUsage NVARCHAR(150),
        ChestScan NVARCHAR(5),
        RaceEthnicityCategory NVARCHAR(60),
        AgeCategory NVARCHAR(60),
        HeightInMeters DECIMAL(4,2),
        WeightInKilograms DECIMAL(6,2),
        BMI DECIMAL(5,2),
        AlcoholDrinkers NVARCHAR(3),
        HIVTesting NVARCHAR(3),
        FluVaxLast12 NVARCHAR(3),
        PneumoVaxEver NVARCHAR(3),
        TetanusLast10Tdap NVARCHAR(150),
        HighRiskLastYear NVARCHAR(5),
        CovidPos NVARCHAR(100)
    );';
    EXEC sys.sp_executesql @sql;

    PRINT N'-- BULK INSERT CSV v�o b?ng raw...';
    SET @sql = N'
    BULK INSERT ' + QUOTENAME(@Schema) + N'.' + QUOTENAME(@RawTable) + N'
    FROM ''' + REPLACE(@FilePath,'''','''''') + N'''
    WITH (
        FORMAT = ''CSV'',
        FIRSTROW = 2,
        CODEPAGE = ''65001'',
        TABLOCK
    );';
    EXEC sys.sp_executesql @sql;
END
ELSE
BEGIN
    PRINT N'-- ?� c� b?ng raw: ' + @Schema + N'.' + @RawTable;
END
GO


IF OBJECT_ID('dbo.HeartDiseaseCleaned','U') IS NOT NULL
    DROP TABLE dbo.HeartDiseaseCleaned;

SELECT
    State = NULLIF(LTRIM(RTRIM(State)), ''),
    Sex   = CASE WHEN Sex IS NULL THEN NULL
                 WHEN LOWER(Sex) IN ('m','male','1') THEN N'Male'
                 WHEN LOWER(Sex) IN ('f','female','2') THEN N'Female'
                 ELSE N'Other' END,
    GeneralHealth = CASE
        WHEN GeneralHealth IS NULL THEN NULL
        WHEN LOWER(GeneralHealth) LIKE '%excellent%'  THEN N'Excellent'
        WHEN LOWER(GeneralHealth) LIKE '%very good%'  THEN N'Very good'
        WHEN LOWER(GeneralHealth) LIKE '%good%'       THEN N'Good'
        WHEN LOWER(GeneralHealth) LIKE '%fair%'       THEN N'Fair'
        WHEN LOWER(GeneralHealth) LIKE '%poor%'       THEN N'Poor'
        ELSE GeneralHealth END,
    LastCheckupTime = CASE
        WHEN LastCheckupTime IS NULL THEN NULL
        WHEN LOWER(LastCheckupTime) LIKE '%within past year%' THEN N'Within past year'
        WHEN LOWER(LastCheckupTime) LIKE '%1-2%' OR LOWER(LastCheckupTime) LIKE '%1 to 2%' THEN N'1�2 years'
        WHEN LOWER(LastCheckupTime) LIKE '%2-5%' OR LOWER(LastCheckupTime) LIKE '%2 to 5%' THEN N'2�5 years'
        WHEN LOWER(LastCheckupTime) LIKE '%5+%' OR LOWER(LastCheckupTime) LIKE '%5 or more%' THEN N'5+ years'
        WHEN LOWER(LastCheckupTime) LIKE '%never%' THEN N'Never'
        ELSE LastCheckupTime END,
    RaceEthnicityCategory = NULLIF(LTRIM(RTRIM(RaceEthnicityCategory)), ''),
    AgeCategory           = NULLIF(LTRIM(RTRIM(AgeCategory)), ''),

    PhysicalHealthDays = CASE
        WHEN PhysicalHealthDays IS NULL THEN NULL
        WHEN PhysicalHealthDays < 0 OR PhysicalHealthDays > 30 THEN NULL
        WHEN PhysicalHealthDays <> FLOOR(PhysicalHealthDays) THEN NULL
        ELSE TRY_CONVERT(TINYINT, PhysicalHealthDays) END,

    MentalHealthDays = CASE
        WHEN MentalHealthDays IS NULL THEN NULL
        WHEN MentalHealthDays < 0 OR MentalHealthDays > 30 THEN NULL
        WHEN MentalHealthDays <> FLOOR(MentalHealthDays) THEN NULL
        ELSE TRY_CONVERT(TINYINT, MentalHealthDays) END,

    SleepHours = CASE
        WHEN SleepHours IS NULL THEN NULL
        WHEN SleepHours < 0 OR SleepHours > 24 THEN NULL
        ELSE TRY_CONVERT(DECIMAL(4,1), SleepHours) END,

    HeightInMeters     = TRY_CONVERT(DECIMAL(4,2), HeightInMeters),
    WeightInKilograms  = TRY_CONVERT(DECIMAL(6,2), WeightInKilograms),
    BMI                = TRY_CONVERT(DECIMAL(5,2), BMI),

    PhysicalActivities = CASE WHEN PhysicalActivities IS NULL THEN NULL
                              WHEN LOWER(PhysicalActivities) IN ('yes','y','1','true') THEN CONVERT(BIT,1)
                              WHEN LOWER(PhysicalActivities) IN ('no','n','0','false')  THEN CONVERT(BIT,0)
                              ELSE NULL END,

    HadHeartAttack = CASE WHEN HadHeartAttack IS NULL THEN NULL
                          WHEN LOWER(HadHeartAttack) IN ('yes','y','1','true') THEN CONVERT(BIT,1)
                          WHEN LOWER(HadHeartAttack) IN ('no','n','0','false')  THEN CONVERT(BIT,0)
                          ELSE NULL END,
    HadAngina = CASE WHEN HadAngina IS NULL THEN NULL
                     WHEN LOWER(HadAngina) IN ('yes','y','1','true') THEN CONVERT(BIT,1)
                     WHEN LOWER(HadAngina) IN ('no','n','0','false')  THEN CONVERT(BIT,0)
                     ELSE NULL END,
    HadStroke = CASE WHEN HadStroke IS NULL THEN NULL
                     WHEN LOWER(HadStroke) IN ('yes','y','1','true') THEN CONVERT(BIT,1)
                     WHEN LOWER(HadStroke) IN ('no','n','0','false')  THEN CONVERT(BIT,0)
                     ELSE NULL END,
    HadAsthma = CASE WHEN HadAsthma IS NULL THEN NULL
                     WHEN LOWER(HadAsthma) IN ('yes','y','1','true') THEN CONVERT(BIT,1)
                     WHEN LOWER(HadAsthma) IN ('no','n','0','false')  THEN CONVERT(BIT,0)
                     ELSE NULL END,
    HadSkinCancer = CASE WHEN HadSkinCancer IS NULL THEN NULL
                         WHEN LOWER(HadSkinCancer) IN ('yes','y','1','true') THEN CONVERT(BIT,1)
                         WHEN LOWER(HadSkinCancer) IN ('no','n','0','false')  THEN CONVERT(BIT,0)
                         ELSE NULL END,
    HadCOPD = CASE WHEN HadCOPD IS NULL THEN NULL
                   WHEN LOWER(HadCOPD) IN ('yes','y','1','true') THEN CONVERT(BIT,1)
                   WHEN LOWER(HadCOPD) IN ('no','n','0','false')  THEN CONVERT(BIT,0)
                   ELSE NULL END,
    HadDepressiveDisorder = CASE WHEN HadDepressiveDisorder IS NULL THEN NULL
                   WHEN LOWER(HadDepressiveDisorder) IN ('yes','y','1','true') THEN CONVERT(BIT,1)
                   WHEN LOWER(HadDepressiveDisorder) IN ('no','n','0','false')  THEN CONVERT(BIT,0)
                   ELSE NULL END,
    HadKidneyDisease = CASE WHEN HadKidneyDisease IS NULL THEN NULL
                   WHEN LOWER(HadKidneyDisease) IN ('yes','y','1','true') THEN CONVERT(BIT,1)
                   WHEN LOWER(HadKidneyDisease) IN ('no','n','0','false')  THEN CONVERT(BIT,0)
                   ELSE NULL END,
    HadArthritis = CASE WHEN HadArthritis IS NULL THEN NULL
                   WHEN LOWER(HadArthritis) IN ('yes','y','1','true') THEN CONVERT(BIT,1)
                   WHEN LOWER(HadArthritis) IN ('no','n','0','false')  THEN CONVERT(BIT,0)
                   ELSE NULL END,

    HadDiabetesLabel = NULLIF(LTRIM(RTRIM(HadDiabetes)), ''),
    DiabetesFlag = CASE
        WHEN HadDiabetes IS NULL THEN NULL
        WHEN LOWER(HadDiabetes) IN ('no','0','false') THEN CONVERT(BIT,0)
        WHEN LOWER(HadDiabetes) LIKE '%yes%' OR LOWER(HadDiabetes) LIKE '%type%' OR LOWER(HadDiabetes) LIKE '%gest%' THEN CONVERT(BIT,1)
        ELSE NULL END,

    DeafOrHardOfHearing = CASE WHEN DeafOrHardOfHearing IS NULL THEN NULL
                               WHEN LOWER(DeafOrHardOfHearing) IN ('yes','y','1','true') THEN CONVERT(BIT,1)
                               WHEN LOWER(DeafOrHardOfHearing) IN ('no','n','0','false')  THEN CONVERT(BIT,0)
                               ELSE NULL END,
    BlindOrVisionDifficulty = CASE WHEN BlindOrVisionDifficulty IS NULL THEN NULL
                               WHEN LOWER(BlindOrVisionDifficulty) IN ('yes','y','1','true') THEN CONVERT(BIT,1)
                               WHEN LOWER(BlindOrVisionDifficulty) IN ('no','n','0','false')  THEN CONVERT(BIT,0)
                               ELSE NULL END,
    DifficultyConcentrating = CASE WHEN DifficultyConcentrating IS NULL THEN NULL
                               WHEN LOWER(DifficultyConcentrating) IN ('yes','y','1','true') THEN CONVERT(BIT,1)
                               WHEN LOWER(DifficultyConcentrating) IN ('no','n','0','false')  THEN CONVERT(BIT,0)
                               ELSE NULL END,
    DifficultyWalking = CASE WHEN DifficultyWalking IS NULL THEN NULL
                               WHEN LOWER(DifficultyWalking) IN ('yes','y','1','true') THEN CONVERT(BIT,1)
                               WHEN LOWER(DifficultyWalking) IN ('no','n','0','false')  THEN CONVERT(BIT,0)
                               ELSE NULL END,
    DifficultyDressingBathing = CASE WHEN DifficultyDressingBathing IS NULL THEN NULL
                               WHEN LOWER(DifficultyDressingBathing) IN ('yes','y','1','true') THEN CONVERT(BIT,1)
                               WHEN LOWER(DifficultyDressingBathing) IN ('no','n','0','false')  THEN CONVERT(BIT,0)
                               ELSE NULL END,

    DifficultyErrands = NULLIF(LTRIM(RTRIM(DifficultyErrands)), ''),
    SmokerStatus      = NULLIF(LTRIM(RTRIM(SmokerStatus)), ''),
    ECigaretteUsage   = NULLIF(LTRIM(RTRIM(ECigaretteUsage)), ''),

    AlcoholDrinkers = CASE WHEN AlcoholDrinkers IS NULL THEN NULL
                           WHEN LOWER(AlcoholDrinkers) IN ('yes','y','1','true') THEN CONVERT(BIT,1)
                           WHEN LOWER(AlcoholDrinkers) IN ('no','n','0','false')  THEN CONVERT(BIT,0)
                           ELSE NULL END,
    HIVTesting        = NULLIF(LTRIM(RTRIM(HIVTesting)), ''),
    FluVaxLast12 = CASE WHEN FluVaxLast12 IS NULL THEN NULL
                        WHEN LOWER(FluVaxLast12) IN ('yes','y','1','true') THEN CONVERT(BIT,1)
                        WHEN LOWER(FluVaxLast12) IN ('no','n','0','false')  THEN CONVERT(BIT,0)
                        ELSE NULL END,
    PneumoVaxEver = CASE WHEN PneumoVaxEver IS NULL THEN NULL
                        WHEN LOWER(PneumoVaxEver) IN ('yes','y','1','true') THEN CONVERT(BIT,1)
                        WHEN LOWER(PneumoVaxEver) IN ('no','n','0','false')  THEN CONVERT(BIT,0)
                        ELSE NULL END,
    TetanusLast10Tdap = NULLIF(LTRIM(RTRIM(TetanusLast10Tdap)), ''),
    HighRiskLastYear = CASE WHEN HighRiskLastYear IS NULL THEN NULL
                        WHEN LOWER(HighRiskLastYear) IN ('yes','y','1','true') THEN CONVERT(BIT,1)
                        WHEN LOWER(HighRiskLastYear) IN ('no','n','0','false')  THEN CONVERT(BIT,0)
                        ELSE NULL END,

    CovidPosFlag = CASE WHEN CovidPos IS NULL THEN NULL
                        WHEN LOWER(CovidPos) IN ('yes','y','1','true','positive','pos') THEN CONVERT(BIT,1)
                        WHEN LOWER(CovidPos) IN ('no','n','0','false','negative','neg')  THEN CONVERT(BIT,0)
                        ELSE NULL END,
    CovidPosRaw = CovidPos
INTO dbo.HeartDiseaseCleaned
FROM dbo.HeartDiseaseRaw;
GO
