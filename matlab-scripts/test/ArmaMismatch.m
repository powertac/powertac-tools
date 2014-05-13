function difference = ArmaMismatchAlt(inParam, rmseIn)
% calculates difference between measured arama values and the calculated ARMA values

% check the number of elements in rmseIn
numOfIntervals = length(rmseIn);
if (numOfIntervals <= 1)
    difference = 0;
    return;
end

if (length(inParam) ~= 3)
    difference = 0;
    return;
end

alpha = inParam(1);
beta = inParam(2);
sigmaz = inParam(3);

retval = 0;
for myk = 1:numOfIntervals
    retval = retval + (rmseIn(myk) - rmseARMA(myk)).^2;
end %for myk
difference = retval;

% nested function 1
    function sumAlpha = sumAlphaOverK(k)
        rv = 0;
        for i = 1:k-1
            rv = rv + alpha.^(i-1);
        end %for
        sumAlpha = rv;
    end %function sumAlphaOverK()

% nested function 2
    function vark = varianceK(k)
        sigzsq = sigmaz.^2;
        a2k = alpha.^(2*(k+17-1));
        ab = 1 + (beta.^2) + (2 * alpha.*beta);
        vark = sigzsq * (a2k + (ab * sumAlphaOverK(k+17)));
    end %varianceK()

% nexted function 3
    function rmseCalc = rmseARMA(k)
        rv = varianceK(k);
        rmseCalc = sqrt(rv);
    end % rmseCalc()


end  %end of function
%==================================END OF FUNCTION==================================