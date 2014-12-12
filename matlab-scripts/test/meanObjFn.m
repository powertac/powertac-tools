function b = meanObjFn(m, inputdata)
%UNTITLED Summary of this function goes here
%   Detailed explanation goes here
residuals = inputdata - m;
b = sum(residuals.^2);

end

