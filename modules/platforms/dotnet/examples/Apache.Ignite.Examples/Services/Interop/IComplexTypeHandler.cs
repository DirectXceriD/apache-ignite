﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Apache.Ignite.Examples.Services.Interop
{
    public interface IComplexTypeHandler
    {
        ComplexType handle(ComplexType obj);
    }
}
