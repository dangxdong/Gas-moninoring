calcRMSE = function (pred, y) {
    m = length(pred)
    cost = 1 / m * sum((pred-y)*(pred-y))
    cost = sum(cost)
    sqrt(cost)
}

