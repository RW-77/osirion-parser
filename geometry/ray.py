from geometry.vec3 import Vec3, Point

class Ray:
    """
    Represents a light ray which is traced by the program, which is essentially a parametrization of a
    line in 3D space. Composed of a starting `Point` and a direction `Vector` (of the form alpha(t) = a + tv).
    """

    def __init__(self, origin: Point = None, direction: Vec3 = None):
        self.origin: Point = origin
        self.dir: Vec3 = direction

    def __repr__(self) -> str:
        """String expression for `Ray`."""

        return "{self.__class__.__name__}(origin={self.orig}, direction={self.dir})".format(self=self)

    def at(self, t) -> Vec3:
        """Returns the point at `t` along the `ray`."""

        return self.origin + t*self.dir
