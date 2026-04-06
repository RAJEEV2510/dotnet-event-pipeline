using FluentValidation;
using EventProcessor.Application.DTOs;

namespace EventProcessor.Application.Validators
{
    public class EventValidator : AbstractValidator<EventDto>
    {
        public EventValidator()
        {
            RuleFor(x => x.EventId).NotEmpty();
            RuleFor(x => x.Type).NotEmpty().MaximumLength(255);
            RuleFor(x => x.Payload).NotEmpty();
            RuleFor(x => x.CreatedAt).NotEmpty();
        }
    }
}
