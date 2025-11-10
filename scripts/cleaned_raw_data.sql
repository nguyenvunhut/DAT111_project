USE HeartDiseaseDB;
GO

/*--------------------------------------------------------------------
  (2) TẠO BẢNG CLEANED TỪ RAW (SELECT ... INTO)
  Đây là bước Transform / Làm sạch dữ liệu
--------------------------------------------------------------------*/
SELECT

    /*--------------------------------------------------------------
      CHUẨN HOÁ TEXT: loại bỏ khoảng trắng, chuỗi rỗng => NULL
    --------------------------------------------------------------*/
    State = NULLIF(LTRIM(RTRIM(State)), ''),

    /*--------------------------------------------------------------
      CHUẨN HOÁ GIỚI TÍNH
      Gom m/M/male về 'Male', f/F/female về 'Female'
    --------------------------------------------------------------*/
    Sex = CASE 
            WHEN Sex IS NULL THEN NULL
            WHEN LOWER(Sex) IN ('m','male') THEN N'Male'
            WHEN LOWER(Sex) IN ('f','female') THEN N'Female'
            ELSE N'Other'
          END,

    /*--------------------------------------------------------------
      CHUẨN HOÁ MỨC ĐỘ SỨC KHỎE CHUNG
      Gom các cách viết khác nhau về danh mục chuẩn:
      Excellent / Very good / Good / Fair / Poor
    --------------------------------------------------------------*/
    GeneralHealth = CASE
            WHEN GeneralHealth IS NULL THEN NULL
            WHEN LOWER(GeneralHealth) LIKE '%excellent%'  THEN N'Excellent'
            WHEN LOWER(GeneralHealth) LIKE '%very good%'  THEN N'Very good'
            WHEN LOWER(GeneralHealth) LIKE '%good%'       THEN N'Good'
            WHEN LOWER(GeneralHealth) LIKE '%fair%'       THEN N'Fair'
            WHEN LOWER(GeneralHealth) LIKE '%poor%'       THEN N'Poor'
            ELSE GeneralHealth END,

    /*--------------------------------------------------------------
      CHUẨN HOÁ THỜI GIAN LẦN KHÁM GẦN NHẤT
      Gom các dạng khác nhau thành 5 nhóm
    --------------------------------------------------------------*/
    LastCheckupTime = CASE
            WHEN LastCheckupTime IS NULL THEN NULL
            WHEN LOWER(LastCheckupTime) LIKE '%within past year%' THEN N'Within past year'
            WHEN LOWER(LastCheckupTime) LIKE '%1-2%' OR LOWER(LastCheckupTime) LIKE '%1 to 2%' THEN N'1-2 years'
            WHEN LOWER(LastCheckupTime) LIKE '%2-5%' OR LOWER(LastCheckupTime) LIKE '%2 to 5%' THEN N'2-5 years'
            WHEN LOWER(LastCheckupTime) LIKE '%5+%' OR LOWER(LastCheckupTime) LIKE '%5 or more%' THEN N'5+ years'
            WHEN LOWER(LastCheckupTime) LIKE '%never%' THEN N'Never'
            ELSE LastCheckupTime END,

    RaceEthnicityCategory = NULLIF(LTRIM(RTRIM(RaceEthnicityCategory)), ''),
    AgeCategory           = NULLIF(LTRIM(RTRIM(AgeCategory)), ''),

    /*--------------------------------------------------------------
      LÀM SẠCH PHYSICAL + MENTAL HEALTH DAYS
      - Chỉ nhận số nguyên 0–30
      - Ngoài khoảng => NULL
    --------------------------------------------------------------*/
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

    /*--------------------------------------------------------------
      LÀM SẠCH SLEEP HOURS
      - Chỉ nhận 0–24
    --------------------------------------------------------------*/
    SleepHours = CASE
        WHEN SleepHours IS NULL THEN NULL
        WHEN SleepHours < 0 OR SleepHours > 24 THEN NULL
        ELSE TRY_CONVERT(DECIMAL(4,1), SleepHours) END,

    /*--------------------------------------------------------------
      ÉP KIỂU CÁC CHỈ SỐ CHIỀU CAO / CÂN NẶNG / BMI
    --------------------------------------------------------------*/
    HeightInMeters     = TRY_CONVERT(DECIMAL(4,2), HeightInMeters),
    WeightInKilograms  = TRY_CONVERT(DECIMAL(6,2), WeightInKilograms),
    BMI                = TRY_CONVERT(DECIMAL(5,2), BMI),

    /*--------------------------------------------------------------
      CÁC CỘT YES/NO → BIT (0/1)
      Lưu ý: dữ liệu raw đã đổi 0/1 thành yes/no
      => chỉ nhận yes/no, true/false
    --------------------------------------------------------------*/
    PhysicalActivities = CASE 
            WHEN PhysicalActivities IS NULL THEN NULL
            WHEN LOWER(PhysicalActivities) IN ('yes','y','true') THEN 1
            WHEN LOWER(PhysicalActivities) IN ('no','n','false') THEN 0
            ELSE NULL END,

    /*--------------------------------------------------------------
      NHÓM BỆNH NỀN TIM MẠCH / HÔ HẤP / TÂM LÝ
    --------------------------------------------------------------*/
    HadHeartAttack = CASE 
            WHEN HadHeartAttack IS NULL THEN NULL
            WHEN LOWER(HadHeartAttack) IN ('yes','y','true') THEN 1
            WHEN LOWER(HadHeartAttack) IN ('no','n','false') THEN 0
            ELSE NULL END,

    HadAngina = CASE 
            WHEN HadAngina IS NULL THEN NULL
            WHEN LOWER(HadAngina) IN ('yes','y','true') THEN 1
            WHEN LOWER(HadAngina) IN ('no','n','false') THEN 0
            ELSE NULL END,

    HadStroke = CASE 
            WHEN HadStroke IS NULL THEN NULL
            WHEN LOWER(HadStroke) IN ('yes','y','true') THEN 1
            WHEN LOWER(HadStroke) IN ('no','n','false') THEN 0
            ELSE NULL END,

    HadAsthma = CASE 
            WHEN HadAsthma IS NULL THEN NULL
            WHEN LOWER(HadAsthma) IN ('yes','y','true') THEN 1
            WHEN LOWER(HadAsthma) IN ('no','n','false') THEN 0
            ELSE NULL END,

    HadSkinCancer = CASE 
            WHEN HadSkinCancer IS NULL THEN NULL
            WHEN LOWER(HadSkinCancer) IN ('yes','y','true') THEN 1
            WHEN LOWER(HadSkinCancer) IN ('no','n','false') THEN 0
            ELSE NULL END,

    HadCOPD = CASE 
            WHEN HadCOPD IS NULL THEN NULL
            WHEN LOWER(HadCOPD) IN ('yes','y','true') THEN 1
            WHEN LOWER(HadCOPD) IN ('no','n','false') THEN 0
            ELSE NULL END,

    HadDepressiveDisorder = CASE 
            WHEN HadDepressiveDisorder IS NULL THEN NULL
            WHEN LOWER(HadDepressiveDisorder) IN ('yes','y','true') THEN 1
            WHEN LOWER(HadDepressiveDisorder) IN ('no','n','false') THEN 0
            ELSE NULL END,

    HadKidneyDisease = CASE 
            WHEN HadKidneyDisease IS NULL THEN NULL
            WHEN LOWER(HadKidneyDisease) IN ('yes','y','true') THEN 1
            WHEN LOWER(HadKidneyDisease) IN ('no','n','false') THEN 0
            ELSE NULL END,

    HadArthritis = CASE 
            WHEN HadArthritis IS NULL THEN NULL
            WHEN LOWER(HadArthritis) IN ('yes','y','true') THEN 1
            WHEN LOWER(HadArthritis) IN ('no','n','false') THEN 0
            ELSE NULL END,

    /*--------------------------------------------------------------
      XỬ LÝ ĐẶC BIỆT: DIABETES
      - Giữ Label gốc để phân tích nâng cao
      - Tạo Flag 0/1 để phân tích thống kê
    --------------------------------------------------------------*/
    HadDiabetesLabel = NULLIF(LTRIM(RTRIM(HadDiabetes)), ''),
    DiabetesFlag = CASE
        WHEN HadDiabetes IS NULL THEN NULL
        WHEN LOWER(HadDiabetes) IN ('no','false') THEN 0
        WHEN LOWER(HadDiabetes) LIKE '%yes%' 
          OR LOWER(HadDiabetes) LIKE '%type%' 
          OR LOWER(HadDiabetes) LIKE '%gest%' THEN 1
        ELSE NULL END,

    /*--------------------------------------------------------------
      NHÓM KHÓ KHĂN HOẠT ĐỘNG HẰNG NGÀY
    --------------------------------------------------------------*/
    DeafOrHardOfHearing = CASE 
            WHEN DeafOrHardOfHearing IS NULL THEN NULL
            WHEN LOWER(DeafOrHardOfHearing) IN ('yes','y','true') THEN 1
            WHEN LOWER(DeafOrHardOfHearing) IN ('no','n','false') THEN 0
            ELSE NULL END,

    BlindOrVisionDifficulty = CASE 
            WHEN BlindOrVisionDifficulty IS NULL THEN NULL
            WHEN LOWER(BlindOrVisionDifficulty) IN ('yes','y','true') THEN 1
            WHEN LOWER(BlindOrVisionDifficulty) IN ('no','n','false') THEN 0
            ELSE NULL END,

    DifficultyConcentrating = CASE
            WHEN DifficultyConcentrating IS NULL THEN NULL
            WHEN LOWER(DifficultyConcentrating) IN ('yes','y','true') THEN 1
            WHEN LOWER(DifficultyConcentrating) IN ('no','n','false') THEN 0
            ELSE NULL END,

    DifficultyWalking = CASE
            WHEN DifficultyWalking IS NULL THEN NULL
            WHEN LOWER(DifficultyWalking) IN ('yes','y','true') THEN 1
            WHEN LOWER(DifficultyWalking) IN ('no','n','false') THEN 0
            ELSE NULL END,

    DifficultyDressingBathing = CASE
            WHEN DifficultyDressingBathing IS NULL THEN NULL
            WHEN LOWER(DifficultyDressingBathing) IN ('yes','y','true') THEN 1
            WHEN LOWER(DifficultyDressingBathing) IN ('no','n','false') THEN 0
            ELSE NULL END,

    /*--------------------------------------------------------------
      GIỮ NGUYÊN TEXT CHO CÁC CỘT CATEGORICAL
    --------------------------------------------------------------*/
    DifficultyErrands = NULLIF(LTRIM(RTRIM(DifficultyErrands)), ''),
    SmokerStatus      = NULLIF(LTRIM(RTRIM(SmokerStatus)), ''),
    ECigaretteUsage   = NULLIF(LTRIM(RTRIM(ECigaretteUsage)), ''),

    /*--------------------------------------------------------------
      NHÓM VACCINE / RISK FACTORS
    --------------------------------------------------------------*/
    AlcoholDrinkers = CASE 
            WHEN AlcoholDrinkers IS NULL THEN NULL
            WHEN LOWER(AlcoholDrinkers) IN ('yes','y','true') THEN 1
            WHEN LOWER(AlcoholDrinkers) IN ('no','n','false') THEN 0
            ELSE NULL END,

    HIVTesting        = NULLIF(LTRIM(RTRIM(HIVTesting)), ''),

    FluVaxLast12 = CASE 
            WHEN FluVaxLast12 IS NULL THEN NULL
            WHEN LOWER(FluVaxLast12) IN ('yes','y','true') THEN 1
            WHEN LOWER(FluVaxLast12) IN ('no','n','false') THEN 0
            ELSE NULL END,

    PneumoVaxEver = CASE 
            WHEN PneumoVaxEver IS NULL THEN NULL
            WHEN LOWER(PneumoVaxEver) IN ('yes','y','true') THEN 1
            WHEN LOWER(PneumoVaxEver) IN ('no','n','false') THEN 0
            ELSE NULL END,

    TetanusLast10Tdap = NULLIF(LTRIM(RTRIM(TetanusLast10Tdap)), ''),

    HighRiskLastYear = CASE 
            WHEN HighRiskLastYear IS NULL THEN NULL
            WHEN LOWER(HighRiskLastYear) IN ('yes','y','true') THEN 1
            WHEN LOWER(HighRiskLastYear) IN ('no','n','false') THEN 0
            ELSE NULL END,

    /*--------------------------------------------------------------
      XỬ LÝ CỘT COVID POSITIVE
      - CovidPosFlag = 1/0
      - CovidPosRaw = giữ nguyên text
    --------------------------------------------------------------*/
    CovidPosFlag = CASE 
            WHEN CovidPos IS NULL THEN NULL
            WHEN LOWER(CovidPos) IN ('yes','y','true','positive','pos') THEN 1
            WHEN LOWER(CovidPos) IN ('no','n','false','negative','neg') THEN 0
            ELSE NULL END,

    CovidPosRaw = CovidPos

/*--------------------------------------------------------------
  INTO = Tạo bảng mới từ kết quả SELECT
--------------------------------------------------------------*/
INTO dbo.HeartDiseaseCleaned
FROM dbo.HeartDiseaseRaw;
GO