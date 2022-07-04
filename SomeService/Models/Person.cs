namespace SomeService.Models;

internal sealed class Person
{
    public string Name { get; init; } = null!;
    public int Age { get; init; }

    public override string ToString() => $"{{ Name : {Name}, Age : {Age} }}";
}