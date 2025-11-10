USE HeartDiseaseDB;
GO

/*--------------------------------------------------------------------
  (TẠO LẠI) BẢNG CLEANED TỪ RAW
  - MỤC TIÊU: Chỉ LÀM SẠCH chuỗi (trim, rỗng -> NULL),
              CHUẨN HOÁ một số danh mục (GeneralHealth, LastCheckupTime),
              GIỮ NGUYÊN các cột Yes/No.
--------------------------------------------------------------------*/
IF OBJECT_ID('dbo.HeartDiseaseCleaned','U') IS NOT NULL
    DROP TABLE dbo.HeartDiseaseCleaned;
GO

SELECT
    /*--------------------------------------------------------------
      1) CHUẨN HOÁ TEXT: bỏ khoảng trắng 2 đầu, chuỗi rỗng => NULL
    --------------------------------------------------------------*/
    State = NULLIF(LTRIM(RTRIM(State)), ''),

    /*--------------------------------------------------------------
      2) CHUẨN HOÁ GIỚI TÍNH (nếu nguồn có M/F)
    --------------------------------------------------------------*/
    Sex = NULLIF(LTRIM(RTRIM(Sex)), ''),

    /*--------------------------------------------------------------
      3) CHUẨN HOÁ MỨC ĐỘ SỨC KHỎE CHUNG: gom về 5 nhóm chuẩn
         (Excellent / Very good / Good / Fair / Poor)
    --------------------------------------------------------------*/
    GeneralHealth = NULLIF(LTRIM(RTRIM(GeneralHealth)), ''),


    /*--------------------------------------------------------------
      4) CHUẨN HOÁ THỜI ĐIỂM KHÁM GẦN NHẤT: gom về 5 nhóm
         Lưu ý dùng dấu '-' chuẩn để tránh lỗi mã hoá (1-2, 2-5)
    --------------------------------------------------------------*/
    LastCheckupTime = CASE
        WHEN LastCheckupTime IS NULL THEN NULL
        WHEN LOWER(LastCheckupTime) LIKE '%within past year%' THEN N'Within past year'
        WHEN LOWER(LastCheckupTime) LIKE '%1-2%' OR LOWER(LastCheckupTime) LIKE '%1 to 2%' THEN N'1-2 years'
        WHEN LOWER(LastCheckupTime) LIKE '%2-5%' OR LOWER(LastCheckupTime) LIKE '%2 to 5%' THEN N'2-5 years'
        WHEN LOWER(LastCheckupTime) LIKE '%5+%' OR LOWER(LastCheckupTime) LIKE '%5 or more%' THEN N'5+ years'
        WHEN LOWER(LastCheckupTime) LIKE '%never%' THEN N'Never'
        ELSE NULLIF(LTRIM(RTRIM(LastCheckupTime)), '')
    END,

    /*--------------------------------------------------------------
      5) CÁC CỘT PHÂN LOẠI KHÁC: chỉ trim và rỗng -> NULL
    --------------------------------------------------------------*/
    RaceEthnicityCategory = NULLIF(LTRIM(RTRIM(RaceEthnicityCategory)), ''),
    AgeCategory           = NULLIF(LTRIM(RTRIM(AgeCategory)), ''),

    /*--------------------------------------------------------------
      6) KIỂM TRA/GIỚI HẠN GIÁ TRỊ SỐ: PHYSICAL/MENTAL DAYS (0–30)
         - Không phải số nguyên trong [0..30] => NULL
    --------------------------------------------------------------*/
    PhysicalHealthDays = CASE
        WHEN TRY_CONVERT(DECIMAL(10,4), PhysicalHealthDays) IS NULL THEN NULL
        WHEN PhysicalHealthDays < 0 OR PhysicalHealthDays > 30 THEN NULL
        WHEN PhysicalHealthDays <> FLOOR(PhysicalHealthDays) THEN NULL
        ELSE TRY_CONVERT(TINYINT, PhysicalHealthDays)
    END,

    MentalHealthDays = CASE
        WHEN TRY_CONVERT(DECIMAL(10,4), MentalHealthDays) IS NULL THEN NULL
        WHEN MentalHealthDays < 0 OR MentalHealthDays > 30 THEN NULL
        WHEN MentalHealthDays <> FLOOR(MentalHealthDays) THEN NULL
        ELSE TRY_CONVERT(TINYINT, MentalHealthDays)
    END,

    /*--------------------------------------------------------------
      7) SLEEP HOURS: chỉ nhận 0–24, ngoài khoảng => NULL
    --------------------------------------------------------------*/
    SleepHours = CASE
        WHEN TRY_CONVERT(DECIMAL(10,4), SleepHours) IS NULL THEN NULL
        WHEN SleepHours < 0 OR SleepHours > 24 THEN NULL
        ELSE TRY_CONVERT(DECIMAL(4,1), SleepHours)
    END,

    /*--------------------------------------------------------------
      8) ÉP KIỂU CHIỀU CAO / CÂN NẶNG / BMI (có thể NULL nếu lỗi)
    --------------------------------------------------------------*/
    HeightInMeters     = TRY_CONVERT(DECIMAL(4,2), HeightInMeters),
    WeightInKilograms  = TRY_CONVERT(DECIMAL(6,2), WeightInKilograms),
    BMI                = TRY_CONVERT(DECIMAL(5,2), BMI),

    /*--------------------------------------------------------------
      9) NHÓM CÁC CỘT DẠNG YES/NO: GIỮ NGUYÊN VĂN BẢN (không đổi)
         - CHỈ TRIM + rỗng -> NULL
         - Không ép sang bit 0/1, không chuẩn hoá về "Yes"/"No"
    --------------------------------------------------------------*/
    PhysicalActivities       = NULLIF(LTRIM(RTRIM(PhysicalActivities)), ''),
    HadHeartAttack           = NULLIF(LTRIM(RTRIM(HadHeartAttack)), ''),
    HadAngina                = NULLIF(LTRIM(RTRIM(HadAngina)), ''),
    HadStroke                = NULLIF(LTRIM(RTRIM(HadStroke)), ''),
    HadAsthma                = NULLIF(LTRIM(RTRIM(HadAsthma)), ''),
    HadSkinCancer            = NULLIF(LTRIM(RTRIM(HadSkinCancer)), ''),
    HadCOPD                  = NULLIF(LTRIM(RTRIM(HadCOPD)), ''),
    HadDepressiveDisorder    = NULLIF(LTRIM(RTRIM(HadDepressiveDisorder)), ''),
    HadKidneyDisease         = NULLIF(LTRIM(RTRIM(HadKidneyDisease)), ''),
    HadArthritis             = NULLIF(LTRIM(RTRIM(HadArthritis)), ''),
    DeafOrHardOfHearing      = NULLIF(LTRIM(RTRIM(DeafOrHardOfHearing)), ''),
    BlindOrVisionDifficulty  = NULLIF(LTRIM(RTRIM(BlindOrVisionDifficulty)), ''),
    DifficultyConcentrating  = NULLIF(LTRIM(RTRIM(DifficultyConcentrating)), ''),
    DifficultyWalking        = NULLIF(LTRIM(RTRIM(DifficultyWalking)), ''),
    DifficultyDressingBathing= NULLIF(LTRIM(RTRIM(DifficultyDressingBathing)), ''),
    AlcoholDrinkers          = NULLIF(LTRIM(RTRIM(AlcoholDrinkers)), ''),
    FluVaxLast12             = NULLIF(LTRIM(RTRIM(FluVaxLast12)), ''),
    PneumoVaxEver            = NULLIF(LTRIM(RTRIM(PneumoVaxEver)), ''),
    HighRiskLastYear         = NULLIF(LTRIM(RTRIM(HighRiskLastYear)), ''),

    /*--------------------------------------------------------------
      10) CÁC CỘT TEXT KHÁC: chỉ trim và rỗng -> NULL
    --------------------------------------------------------------*/
    DifficultyErrands  = NULLIF(LTRIM(RTRIM(DifficultyErrands)), ''),
    SmokerStatus       = NULLIF(LTRIM(RTRIM(SmokerStatus)), ''),
    ECigaretteUsage    = NULLIF(LTRIM(RTRIM(ECigaretteUsage)), ''),
    HIVTesting         = NULLIF(LTRIM(RTRIM(HIVTesting)), ''),
    TetanusLast10Tdap  = NULLIF(LTRIM(RTRIM(TetanusLast10Tdap)), ''),

    /*--------------------------------------------------------------
      11) COVID POSITIVE: GIỮ NGUYÊN văn bản (Yes/No/Positive/Negative…)
          - Không chuyển sang 0/1, chỉ trim
    --------------------------------------------------------------*/
    CovidPos = NULLIF(LTRIM(RTRIM(CovidPos)), ''),

    /*--------------------------------------------------------------
      12) GIỮ NHÃN GỐC CHO DIABETES (đôi khi có "Type I/II", "Gestational")
          - Không tạo cờ 0/1 ở phiên bản này theo yêu cầu
    --------------------------------------------------------------*/
    HadDiabetes = NULLIF(LTRIM(RTRIM(HadDiabetes)), '')

INTO dbo.HeartDiseaseCleaned
FROM dbo.HeartDiseaseRaw;
GO
