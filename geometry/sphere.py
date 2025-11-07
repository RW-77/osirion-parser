import math
from geometry.vec3 import Vec3, Point, dot
from geometry.ray import Ray

class Sphere:
    """Represents a sphere with center `self.center` and radius `self.radius` of material `mat`."""
    def __init__(self, center: Point, radius: float) -> None:
        self.center = center
        self.radius = radius

    def hit(self, _r: Ray, max_len: float) -> float | None:
        """
        Returns true if hit by the ray any t within an interval, and updates the hit record information
        accordingly in O(1) time.
        Calculates the surface normal, whether the sphere was hit from the outside, the point and time (t)
        of contact.
        """
        oc = _r.origin - self.center
        a = _r.dir.length_squared()
        half_b = dot(oc, _r.dir)
        c = oc.length_squared() - self.radius * self.radius

        disc = half_b * half_b - a * c
        if disc < 0:
            return None

        sqrtd = math.sqrt(disc)
        root = (-half_b - sqrtd) / a
        if root <= 0:
            root = (-half_b + sqrtd) / a
            if root <= 0:
                return None

        return root if (root <= max_len) else None

def test_case(desc, origin, dir, center, radius, max_len=100):
    ray = Ray(Point(*origin), Vec3(*dir))
    sphere = Sphere(Point(*center), radius)
    result = sphere.hit(ray, max_len)
    print(f"{desc:35} -> {result}")

if __name__ == "__main__":
    '''
    center = Point(1, 1, 1)
    radius = 1
    test_sphere = Sphere(center, radius)
    ray = Ray(Point(1,2,1), Vec3(1,2,3))
    result = test_sphere.hit(ray, 300)
    print(result)
    '''

    test_case(
        "Miss — ray goes above sphere",
        origin=(0, 0.5, 0),
        dir=(2.6, 1.2, 2.2),
        center=(1, 1, 1),
        radius=0.1
    )
