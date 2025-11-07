import sys
import random
import math
from typing import Any

class Vec3:
    """
    A 3-dimensional vector.
    """

    te = "Unsupported operand type for {op}."

    def __init__(self, x: float = 0, y : float = 0, z : float = 0):
        self.x = x
        self.y = y
        self.z = z

    def __repr__(self):
        """String expression for `vec3`"""

        return "{self.__class__.__name__}(x={self.x}, y={self.y}, z={self.z})".format(self=self)

    def __str__(self):
        """String representation of 'vec3'"""

        return "({self.x}, {self.y}, {self.z})".format(self=self)

    def __neg__(self):
        """Performs element-wise negation."""

        return Vec3(-self.x, -self.y, -self.z)
    
    def __add__(self, v):
        """Performs element-wise vector addition."""

        if isinstance(v, Vec3):
            return Vec3(self.x + v.x, self.y + v.y, self.z + v.z)
        else:
            raise TypeError(Vec3.te.format(op="vector addition") + f": {type(v)}")

    # def __radd__(self, v):
    #     """Performs element-wise vector addition."""

    #     if isinstance(v, vec3):
    #         return vec3(self.x + v.x, self.y + v.y, self.z + v.z)
    #     else:
    #         raise TypeError(vec3.te.format(op="vector addition") + f": {type(v)}")

    def __iadd__(self, v):
        """Performs in-place element-wise vector addition."""

        if isinstance(v, Vec3):
            self.x += v.x
            self.y += v.y
            self.z += v.z
        else:
            raise TypeError(Vec3.te.format(op="in-place vector addition"))

    def __sub__(self, v):
        """Performs element-wise vector subtraction."""

        if isinstance(v, Vec3):
            return Vec3(self.x - v.x, self.y - v.y, self.z - v.z)
        else:
            raise TypeError(Vec3.te.format(op="vector subtraction"))

    def __rsub__(self, v):
        """Performs element-wise vector subtraction."""

        return self.__sub__(v)

    def __isub__(self, v):
        """Performs in-place element-wise vector subtraction."""

        if isinstance(v, Vec3):
            self.x -= v.x
            self.y -= v.y
            self.z -= v.z
        else:
            raise TypeError(Vec3.te.format(op="in-place vector subtraction"))

    def __mul__(self, v):
        """Performs element-wise vector multiplication."""

        if isinstance(v, (int, float)):
            return Vec3(self.x * v, self.y * v, self.z * v)
        if isinstance(v, Vec3):
            return Vec3(self.x * v.x, self.y * v.y, self.z * v.z)
        else:
            raise TypeError(Vec3.te.format(op="scalar/vector multiplication"))

    def __rmul__(self, v):
        """Performs element-wise vector multiplication."""

        return self.__mul__(v)

    def __imul__(self, s):
        """Performs in-place scalar multiplication."""

        if isinstance(s, (int, float)):
            self.x *= s
            self.y *= s
            self.z *= s
            return self
        else:
            raise TypeError(Vec3.te.format(op="in-place scalar multiplication"))

    def __truediv__(self, v):
        """Performs element-wise vector division."""

        if isinstance(v, (int, float)):
            if v != 0:
                return self * (1 / v)
            else:
                raise ValueError("Unsupported: division by 0.")
        else:
            raise TypeError(Vec3.te.format(op="scalar division"))
        
    # in-place scalar division
    def __itruediv__(self, s):
        """Performs in-place element-wise vector division."""

        if isinstance(s, (int, float)):
            if s != 0:
                return self.__imul__(1 / s)
            else:
                raise ValueError("Unsupported: diviwsion by 0.")
        else:
            raise TypeError(Vec3.te.format(op="in-place scalar division"))

    # NOTE: consider precomputing as member attribute
    def length(self):
        """Returns the magnitude of this vector."""

        return math.sqrt(dot(self, self))
    
    # magnitude squared
    def length_squared(self):
        """Returns the square of the magnitude of this vector."""

        return dot(self, self)
    
    def near_zero(self) -> bool:
        """Returns true if all components of this vector are sufficiently close to 0."""

        s: float = 1e-8
        return abs(self.x) < s and abs(self.y) < s and abs(self.z) < s

# from utils import Interval

# RGB and Point are aliases for vec3
global RGB, Point
RGB = Point = Vec3

def dot(v1, v2):
    """Returns the dot product of `v1` and `v2`."""

    return v1.x*v2.x + v1.y*v2.y + v1.z*v2.z

def cross(v1, v2):
    """Returns the cross product of `v1` and `v2`."""

    return Vec3(v1.y * v2.z - v1.z * v2.y, -v1.x * v2.z + v1.z * v2.x, v1.x * v2.y - v1.y * v2.x)

def normalize(v):
    """Returns the norm of a vector."""
    return v / v.length()

