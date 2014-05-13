rmseMeas = [0.696394, 0.700392, 0.68845, 0.696045, 0.684595, 0.693628...
, 0.740111...
, 0.71722, 0.742998, 0.793031, 0.818331, 0.840281, 0.854741...
, 0.916607, 0.947754, 0.966716, 0.979394, 0.979576, 0.983366, 0.976213...
, 0.993308, 1.008888, 1.022134, 1.028177]

alpha0 = 1.0;
beta0 = 0.04;
sigmaz0 = 0.15;

inP(1) = alpha0;
inP(2) = beta0;
inP(3) = sigmaz0;
sol = ArmaMismatch(inP, rmseMeas)

   options = optimset('TolX',1e-6,'TolFun',1e-6);
[optimalVals act exit] = fminunc(@(x) ArmaMismatchAlt(x, rmseMeas), inP)



sol = ArmaMismatch(optimalVals, rmseMeas)


inP = [1.0073, 0.0327, 0.1372]

sol = ArmaMismatch(inP, rmseMeas)

